# -*- coding: utf-8 -*-
from doorman.database import db
from doorman.extensions import cache
from doorman.models import DistributedQuery, DistributedQueryTask


def add_distributed_query(sql, description, not_before, *nodes):
    query = DistributedQuery.create(
        sql=sql,
        description=description,
        not_before=not_before
    )
    create_distributed_query_tasks_for_query(query, *nodes)
    return query


def create_distributed_query_tasks_for_query(query, *nodes):

    mapping = {
        'sql': query.sql,
        'not_before': query.not_before.strftime("%s.%f")

    }

    for node in nodes:
        cache.redis.sadd('doorman:distributed_queries_by_node:{0}'.format(node.id), query.id)
        task = DistributedQueryTask(node=node, distributed_query=query)
        mapping[node.id] = task.guid
        db.session.add(task)
    else:
        cache.redis.hmset('doorman:distributed_query:{0}'.format(query.id), mapping)
        db.session.commit()

    return
