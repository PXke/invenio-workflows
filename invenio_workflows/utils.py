# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2012, 2013, 2014, 2015, 2016 CERN.
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

"""Various utility functions for use across the workflows module."""

from functools import wraps

from flask import current_app, jsonify
from operator import attrgetter

from six import text_type


class BibWorkflowObjectIdContainer(object):

    """Mapping from an ID to DbWorkflowObject.

    This class is only used to be able to store a workflow ID and
    to retrieve easily the workflow from this ID from another process,
    such as a Celery worker process.

    It is used mainly to avoid problems with SQLAlchemy sessions
    when we use different processes.
    """

    def __init__(self, bibworkflowobject=None):
        """Initialize the object, optionally passing a DbWorkflowObject."""
        if bibworkflowobject is not None:
            self.id = bibworkflowobject.id
        else:
            self.id = None

    def get_object(self):
        """Get the DbWorkflowObject from self.id."""
        from invenio_workflows.models import DbWorkflowObject

        if self.id is not None:
            return DbWorkflowObject.query.filter(
                DbWorkflowObject.id == self.id
            ).one()
        else:
            return None

    def from_dict(self, dict_to_process):
        """Take a dict with special keys and set the current id.

        :param dict_to_process: dict created before with to_dict()
        :type dict_to_process: dict

        :return: self, BibWorkflowObjectIdContainer.
        """
        self.id = dict_to_process[str(self.__class__)]["id"]
        return self

    def to_dict(self):
        """Create a dict with special keys for later retrieval."""
        return {str(self.__class__): self.__dict__}

def get_task_history(last_task):
    """Append last task to task history."""
    if hasattr(last_task, 'branch') and last_task.branch:
        return
    elif hasattr(last_task, 'hide') and last_task.hide:
        return
    else:
        return get_func_info(last_task)


def get_func_info(func):
    """Retrieve a function's information."""
    name = func.func_name
    doc = func.func_doc or ""
    try:
        nicename = func.description
    except AttributeError:
        if doc:
            nicename = doc.split('\n')[0]
            if len(nicename) > 80:
                nicename = name
        else:
            nicename = name
    parameters = []
    closure = func.func_closure
    varnames = func.func_code.co_freevars
    if closure:
        for index, arg in enumerate(closure):
            if not callable(arg.cell_contents):
                parameters.append((varnames[index],
                                   text_type(arg.cell_contents)))
    return ({
        "nicename": nicename,
        "doc": doc,
        "parameters": parameters,
        "name": name
    })


def get_workflow_info(func_list):
    """Return function info, go through lists recursively."""
    funcs = []
    for item in func_list:
        if item is None:
            continue
        if isinstance(item, list):
            funcs.append(get_workflow_info(item))
        else:
            funcs.append(get_func_info(item))
    return funcs
