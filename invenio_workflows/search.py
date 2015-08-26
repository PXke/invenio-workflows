# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2015 CERN.
#
# Invenio is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA 02111-1307, USA.

"""Search functions for Holding Pen interface."""

from flask import current_app

from invenio.base.globals import cfg

from invenio_search.api import Query


def search(query, sorting={}):
    """Return a ready Holding Pen search object from query and sorting."""
    results = Query(query)
    response = results.search()
    response.index = cfg["WORKFLOWS_HOLDING_PEN_INDEX"]
    if sorting:
        response.body.update(sorting)
    # FIXME pagination
    response.body["size"] = 99999999
    return response


def get_holdingpen_objects(tags_list=None,
                           sort_key="modified",
                           operator="AND"):
    """Get records for display in Holding Pen."""
    if sort_key.endswith("_desc"):
        order = "desc"
        sort_key = sort_key[:-5]
    else:
        order = "asc"

    if not sort_key:
        sort_key = "modified"

    sorting = {
        "sort": {
            sort_key: {
                "order": order
            }
        }
    }
    current_app.logger.debug(sorting)
    response = search(
        query=" {0} ".format(operator).join(tags_list),
        sorting=sorting
    )
    current_app.logger.debug(response.body)
    # Return hits
    return [int(r['_id']) for r in response._search()['hits']['hits']]
