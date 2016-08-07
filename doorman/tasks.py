# -*- coding: utf-8 -*-
from celery import Celery
from flask import current_app

import json

from doorman.database import db
from doorman.extensions import cache, log_tee
from doorman.models import (
    DistributedQueryResult,
    DistributedQueryTask,
    Node,
    StatusLog
)
import doorman.utils as utils


celery = Celery(__name__)


@celery.task()
def process_result(node_id=None, remote_addr=None, last_checkin=None,
    log_type=None, data=None):

    node = Node.get_by_id(node_id)
    refresh_node(node_id=node_id, remote_addr=remote_addr, last_checkin=last_checkin)

    current_app.logger.debug("Processing %s result from %s", log_type, node)

    if log_type == 'status':
        log_level = current_app.config['DOORMAN_MINIMUM_OSQUERY_LOG_LEVEL']

        for item in data.get('data', []):
            if int(item['severity']) < log_level:
                continue
            status_log = StatusLog(node=node, **item)
            db.session.add(status_log)
        else:
            db.session.commit()

        log_tee.handle_status(data, host_identifier=node.host_identifier)

    elif log_type == 'result':
        db.session.bulk_save_objects(utils.process_result(data, node.id))
        db.session.commit()

        analyze_result.s(data, node.to_dict()).delay()
        learn_from_result.s(data, node.to_dict()).delay()

        log_tee.handle_result(data, host_identifier=node.host_identifier)

    elif log_type == 'distributed':
        for guid, results in data.get('queries', {}).items():

            task = DistributedQueryTask.query.filter(
                DistributedQueryTask.guid == guid,
                DistributedQueryTask.status == DistributedQueryTask.PENDING,
                DistributedQueryTask.node == node,
            ).first()

            if not task:
                current_app.logger.error(
                    "%s - Got result for distributed query not in PENDING "
                    "state: %s: %s",
                    remote_addr, guid, json.dumps(data)
                )
                continue

            for columns in results:
                result = DistributedQueryResult(
                    columns,
                    distributed_query=task.distributed_query,
                    distributed_query_task=task
                )
                db.session.add(result)
            else:
                task.status = DistributedQueryTask.COMPLETE
                db.session.add(task)

        else:
            db.session.commit()

    else:
        current_app.logger.error("%s - Unknown log_type %r",
            remote_addr, log_type
        )
        current_app.logger.info(json.dumps(data))

    return


@celery.task()
def analyze_result(result, node):
    current_app.rule_manager.handle_log_entry(result, node)
    return


@celery.task()
def learn_from_result(result, node):
    utils.learn_from_result(result, node)
    return


@celery.task()
def example_task(one, two):
    print('Adding {0} and {1}'.format(one, two))
    return one + two


@celery.task()
def set_distributed_query_tasks_as_pending(node_id=None, remote_addr=None, last_checkin=None, guids=None):
    node = Node.get_by_id(node_id)
    refresh_node(node_id=node_id, remote_addr=remote_addr, last_checkin=last_checkin)

    result = 0

    if guids:
        current_app.logger.debug("Setting %s to PENDING", ','.join(guids))

        result = db.session.query(DistributedQueryTask) \
            .filter(DistributedQueryTask.guid.in_(guids)) \
            .update({
                'timestamp': last_checkin,
                'status': DistributedQueryTask.PENDING
            }, synchronize_session='fetch')
        db.session.commit()

    return result


@celery.task()
def refresh_node(node_id=None, remote_addr=None, last_checkin=None):
    node = Node.get_by_id(node_id)
    node.update(last_checkin=last_checkin, last_ip=remote_addr)
    utils.refresh_cached_node_expiration(node.node_key, timeout=7200)
    return
