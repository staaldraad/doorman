# -*- coding: utf-8 -*-
from functools import wraps
from io import BytesIO
import datetime as dt
import gzip
import json
import logging
import uuid

from flask import Blueprint, current_app, jsonify, request

from doorman import tasks
from doorman.models import (
    Node, Tag,
    DistributedQueryTask, DistributedQueryResult,
    StatusLog,
)
from doorman.utils import (
    CachedNode,
    assemble_distributed_queries,
    get_cached_node,
    set_cached_node,
    refresh_cached_node_expiration,
)


blueprint = Blueprint('api', __name__)


def node_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # in v1.7.4, the Content-Encoding header is set when
        # --logger_tls_compress=true
        if 'Content-Encoding' in request.headers and \
            request.headers['Content-Encoding'] == 'gzip':
            request._cached_data = gzip.GzipFile(
                fileobj=BytesIO(request.get_data())).read()

        request_json = request.get_json()

        if not request_json or 'node_key' not in request_json:
            current_app.logger.error(
                "%s - Request did not contain valid JSON data. This could "
                "be an attempt to gather information about this endpoint "
                "or an automated scanner.",
                request.remote_addr
            )
            # Return nothing
            return ""

        node_key = request_json.get('node_key')
        node = get_cached_node(node_key)

        if not node:
            node = Node.query.filter_by(node_key=node_key).first()

            if not node:
                current_app.logger.error(
                    "%s - Could not find node with node_key %s",
                    request.remote_addr, node_key
                )
                return jsonify(node_invalid=True)

            if not node.is_active:
                current_app.logger.error(
                    "%s - Node %s came back from the dead!",
                    request.remote_addr, node_key
                )
                return jsonify(node_invalid=True)

            node = node.to_dict()

            # cache node information for two hours
            set_cached_node(node_key, node, timeout=7200)

        return f(node=CachedNode(**node), *args, **kwargs)
    return decorated_function


@blueprint.route('/')
def index():
    return '', 204


@blueprint.route('/enroll', methods=['POST', 'PUT'])
@blueprint.route('/v1/enroll', methods=['POST', 'PUT'])
def enroll():
    '''
    Enroll an endpoint with osquery.

    :returns: a `node_key` unique id. Additionally `node_invalid` will
        be true if the node failed to enroll.
    '''
    request_json = request.get_json()
    if not request_json:
        current_app.logger.error(
            "%s - Request did not contain valid JSON data. This could "
            "be an attempt to gather information about this endpoint "
            "or an automated scanner.",
            request.remote_addr
        )
        # Return nothing
        return ""

    enroll_secret = request_json.get(
        current_app.config.get('DOORMAN_ENROLL_OVERRIDE', 'enroll_secret'))

    if not enroll_secret:
        current_app.logger.error(
            "%s - No enroll_secret provided by remote host",
            request.remote_addr
        )
        return jsonify(node_invalid=True)

    # If we pre-populate node table with a per-node enroll_secret,
    # let's query it now.

    node = Node.query.filter_by(enroll_secret=enroll_secret).first()

    if not node and enroll_secret not in current_app.config['DOORMAN_ENROLL_SECRET']:
        current_app.logger.error("%s - Invalid enroll_secret %s",
            request.remote_addr, enroll_secret
        )
        return jsonify(node_invalid=True)

    host_identifier = request_json.get('host_identifier')

    if node and node.enrolled_on and node.is_active:
        current_app.logger.warn(
            "%s - %s already enrolled on %s, returning existing node_key",
            request.remote_addr, node, node.enrolled_on
        )

        if node.host_identifier != host_identifier:
            current_app.logger.info(
                "%s - %s changed their host_identifier to %s",
                request.remote_addr, node, host_identifier
            )
            node.host_identifier = host_identifier

        node.update(
            last_checkin=dt.datetime.utcnow(),
            last_ip=request.remote_addr
        )

        return jsonify(node_key=node.node_key, node_invalid=False)

    existing_node = None
    if host_identifier:
        existing_node = Node.query.filter(
            Node.host_identifier == host_identifier
        ).first()

    if existing_node and not existing_node.enroll_secret:
        current_app.logger.warning(
            "%s - Duplicate host_identifier %s, already enrolled %s",
            request.remote_addr, host_identifier, existing_node.enrolled_on
        )

        if not existing_node.is_active:
            current_app.logger.error(
                "%s - Node %s came back from the dead!",
                request.remote_addr, existing_node.node_key
            )
            return jsonify(node_invalid=True)

        if current_app.config['DOORMAN_EXPECTS_UNIQUE_HOST_ID'] is True:
            current_app.logger.info(
                "%s - Unique host identification is true, %s already enrolled "
                "returning existing node key %s",
                request.remote_addr, host_identifier, existing_node.node_key
            )
            existing_node.update(
                last_checkin=dt.datetime.utcnow(),
                last_ip=request.remote_addr
            )
            return jsonify(node_key=existing_node.node_key, node_invalid=False)

    now = dt.datetime.utcnow()

    if node:
        node.update(host_identifier=host_identifier,
                    last_checkin=now,
                    enrolled_on=now,
                    last_ip=request.remote_addr)
    else:
        node = Node(host_identifier=host_identifier,
                    last_checkin=now,
                    enrolled_on=now,
                    last_ip=request.remote_addr)

        for value in current_app.config.get('DOORMAN_ENROLL_DEFAULT_TAGS', []):
            tag = Tag.query.filter_by(value=value).first()
            if tag and tag not in node.tags:
                node.tags.append(tag)
            elif not tag:
                node.tags.append(Tag(value=value))

        node.save()

    current_app.logger.info("%s - Enrolled new node %s",
        request.remote_addr, node
    )

    return jsonify(node_key=node.node_key, node_invalid=False)


@blueprint.route('/config', methods=['POST', 'PUT'])
@blueprint.route('/v1/config', methods=['POST', 'PUT'])
@node_required
def configuration(node):
    '''
    Retrieve an osquery configuration for a given node.

    :returns: an osquery configuration file
    '''
    current_app.logger.info(
        "%s - %s checking in to retrieve a new configuration",
        request.remote_addr, node.id
    )

    node = Node.get_by_id(node.id)
    config = node.get_config()

    tasks.refresh_node.delay(
        node_id=node.id,
        remote_addr=request.remote_addr,
        last_checkin=dt.datetime.utcnow()
    )

    return jsonify(config, node_invalid=False)


@blueprint.route('/log', methods=['POST', 'PUT'])
@blueprint.route('/v1/log', methods=['POST', 'PUT'])
@node_required
def logger(node):
    '''
    Ingest results for scheduled queries from an osquery endpoint
    and send it off to a background task to be processed by a
    Celery worker.
    '''
    data = request.get_json()

    current_app.logger.info(
        "%s - %s checking in to log query results",
        request.remote_addr, node.id
    )

    if current_app.logger.isEnabledFor(logging.DEBUG):
        current_app.logger.debug(json.dumps(data, indent=2))

    tasks.process_result.delay(
        node_id=node.id,
        remote_addr=request.remote_addr,
        last_checkin=dt.datetime.utcnow(),
        log_type=data.get('log_type'),
        data=data
    )

    return jsonify(node_invalid=False)


@blueprint.route('/distributed/read', methods=['POST', 'PUT'])
@blueprint.route('/v1/distributed/read', methods=['POST', 'PUT'])
@node_required
def distributed_read(node):
    '''
    '''
    data = request.get_json()

    current_app.logger.info(
        "%s - %s checking in to retrieve distributed queries",
        request.remote_addr, node.id
    )

    node = Node.get_by_id(node.id)
    queries = assemble_distributed_queries(node)

    tasks.set_distributed_query_tasks_as_pending.delay(
        node_id=node.id,
        remote_addr=request.remote_addr,
        last_checkin=dt.datetime.utcnow(),
        guids=list(queries)
    )

    return jsonify(queries=queries, node_invalid=False)


@blueprint.route('/distributed/write', methods=['POST', 'PUT'])
@blueprint.route('/v1/distributed/write', methods=['POST', 'PUT'])
@node_required
def distributed_write(node):
    '''
    Ingest results of a distributed query from an osquery endpoint
    and send it off to a background task to be processed by a
    Celery worker.
    '''
    data = request.get_json()

    current_app.logger.info(
        "%s - %s checking in to log distributed query results",
        request.remote_addr, node.id
    )

    if current_app.logger.isEnabledFor(logging.DEBUG):
        current_app.logger.debug(json.dumps(data, indent=2))

    tasks.process_result.delay(
        node_id=node.id,
        remote_addr=request.remote_addr,
        last_checkin=dt.datetime.utcnow(),
        log_type='distributed',
        data=data
    )

    return jsonify(node_invalid=False)
