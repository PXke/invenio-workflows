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

from __future__ import absolute_import

from six import reraise, text_type
from celery import shared_task

from .worker_result import AsynchronousResultWrapper
from .errors import WorkflowWorkerError


@shared_task
def run(workflow_name, data, **kwargs):
    """Run the workflow with Celery."""

    from .worker_engine import run_worker
    from .utils import BibWorkflowObjectIdContainer

    if isinstance(data, list):
        # For each data item check if dict and then
        # see if the dict contains a BibWorkflowObjectId container
        # generated dict.
        for i in range(0, len(data)):
            if isinstance(data[i], dict):
                if str(BibWorkflowObjectIdContainer().__class__) in data[i]:
                    data[i] = BibWorkflowObjectIdContainer().from_dict(data[
                        i]).get_object()
    else:
        raise WorkflowWorkerError("Data is not a list: %r" % (data,))

    return text_type(run_worker(workflow_name, data, **kwargs).uuid)


@shared_task
def restart(wid, **kwargs):
    """Restart the workflow with Celery."""
    from .worker_engine import restart_worker
    return text_type(restart_worker(wid, **kwargs).uuid)


@shared_task
def resume(oid, restart_point, **kwargs):
    """Restart the workflow with Celery."""
    from .worker_engine import continue_worker
    return text_type(continue_worker(oid, restart_point, **kwargs).uuid)
