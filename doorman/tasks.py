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
def process_result(result_type, result_key):

    current_app.logger.debug("Fetching %s results for %s", result_type, result_key)
    result = cache.get(result_key)

    if not result:
        current_app.logger.error("No data at %s", result_key)
        return

    last_checkin = result.pop('last_checkin')
    remote_addr = result.pop('remote_addr')
    data = result.pop('data')

    node_key = data.get('node_key')
    node = Node.query.filter_by(node_key=node_key).one()
    node.update(last_checkin=last_checkin, last_ip=remote_addr)

    if result_type == 'logger' and data['log_type'] == 'status':
        log_level = current_app.config['DOORMAN_MINIMUM_OSQUERY_LOG_LEVEL']

        for item in data.get('data', []):
            if int(item['severity']) < log_level:
                continue
            status_log = StatusLog(node=node, **item)
            db.session.add(status_log)
        else:
            db.session.commit()

        log_tee.handle_status(data, host_identifier=node.host_identifier)

    elif result_type == 'logger' and data['log_type'] == 'result':
        db.session.bulk_save_objects(utils.process_result(data, node.id))
        db.session.commit()

        analyze_result.s(data, node.to_dict()).delay()
        learn_from_result.s(data, node.to_dict()).delay()

        log_tee.handle_result(data, host_identifier=node.host_identifier)

    elif result_type == 'distributed':
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

    # made it this far, so let's delete the result from cache

    current_app.logger.debug("Deleting result from cache for key %s", result_key)
    cache.delete(result_key)
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
def set_distributed_query_tasks_as_pending(task_key):

    current_app.logger.debug("Fetching new tasks from %s", task_key)
    task = cache.get(task_key)

    if not task:
        current_app.logger.error("No data at %s", task_key)
        return

    node_key = task.pop('node_key')
    last_checkin = task.pop('last_checkin')
    remote_addr = task.pop('remote_addr')
    guids = task.pop('guids')

    current_app.logger.debug("Setting %s to PENDING", ','.join(guids))

    node = Node.query.filter_by(node_key=node_key).one()
    node.update(last_checkin=last_checkin, last_ip=remote_addr)

    result = db.session.query(DistributedQueryTask) \
        .filter(DistributedQueryTask.guid.in_(guids)) \
        .update({
            'timestamp': last_checkin,
            'status': DistributedQueryTask.PENDING
        }, synchronize_session='fetch')
    db.session.commit()

    current_app.logger.debug("Deleting new tasks from cache for key %s", task_key)
    cache.delete(task_key)
    return result
