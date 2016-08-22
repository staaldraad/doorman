# -*- coding: utf-8 -*-
from doorman.database import db
from doorman.models import DistributedQuery, DistributedQueryTask


def add_distributed_query(sql, description, not_before, *nodes):

    query = DistributedQuery.create(
        sql=sql,
        description=description,
        not_before=not_before
    )

    for node in nodes:
        task = DistributedQueryTask(node=node, distributed_query=query)
        db.session.add(task)
    else:
        db.session.commit()

    return query
