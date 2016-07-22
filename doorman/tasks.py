# -*- coding: utf-8 -*-
from celery import Celery
from flask import current_app

import json


from doorman.database import db
from doorman.extensions import cache, log_tee
from doorman.models import Node, StatusLog
import doorman.utils as utils


celery = Celery(__name__)


@celery.task()
def process_result(result_key):

    current_app.logger.debug("Fetching results for %s", result_key)
    result = cache.get(result_key)

    try:
        if not result:
            current_app.logger.error("No data at %s", result_key)
            return

        last_checkin = result.pop('last_checkin')
        remote_addr = result.pop('remote_addr')
        data = result.pop('data')

        node_key = data.get('node_key')
        node = Node.query.filter_by(node_key=node_key).one()
        node.update(last_checkin=last_checkin, last_ip=remote_addr)

        log_type = data['log_type']
        log_level = current_app.config['DOORMAN_MINIMUM_OSQUERY_LOG_LEVEL']

        if log_type == 'status':
            log_tee.handle_status(data, host_identifier=node.host_identifier)
            for item in data.get('data', []):
                if int(item['severity']) < log_level:
                    continue
                status_log = StatusLog(node=node, **item)
                db.session.add(status_log)
            else:
                db.session.commit()

        elif log_type == 'result':
            db.session.bulk_save_objects(utils.process_result(data, node.id))
            db.session.commit()

            log_tee.handle_result(data, host_identifier=node.host_identifier)
            analyze_result.s(data, node.to_dict()).delay()
            learn_from_result.s(data, node.to_dict()).delay()

        else:
            current_app.logger.error("%s - Unknown log_type %r",
                remote_addr, log_type
            )
            current_app.logger.info(json.dumps(data))

    except Exception:
        current_app.logger.exception(
            "Error processing %s: %s, leaving result in cache",
            result_key,
            result
        )

    else:
        current_app.logger.debug(
            "Deleting result from cache for key %s",
            result_key
        )
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
