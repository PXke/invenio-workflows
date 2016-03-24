# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016 CERN.
#
# Invenio is free software; you can redistribute it
# and/or modify it under the terms of the GNU General Public License as
# published by the Free Software Foundation; either version 2 of the
# License, or (at your option) any later version.
#
# Invenio is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Invenio; if not, write to the
# Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
# MA 02111-1307, USA.
#
# In applying this license, CERN does not
# waive the privileges and immunities granted to it by virtue of its status
# as an Intergovernmental Organization or submit itself to any jurisdiction.


"""Module tests."""

from __future__ import absolute_import, print_function

from flask import Flask

from demo_package.workflows.demo_workflow import demo_workflow

from invenio_workflows import InvenioWorkflows, start

from workflow.engine_db import WorkflowStatus


def test_version():
    """Test version import."""
    from invenio_workflows import __version__
    assert __version__


def test_init():
    """Test extension initialization."""
    app = Flask('testapp')
    ext = InvenioWorkflows(app)
    assert 'invenio-workflows' in app.extensions
    ext.register_workflow('test_workflow', demo_workflow)
    assert 'test_workflow' in app.extensions['invenio-workflows'].workflows

    app = Flask('testapp')
    ext = InvenioWorkflows()
    assert 'invenio-workflows' not in app.extensions
    ext.init_app(app)
    assert 'invenio-workflows' in app.extensions


def test_halt(app, halt_workflow, halt_workflow_conditional):
    """Test halt task."""
    from invenio_workflows import DbWorkflowObject

    app.extensions['invenio-workflows'].register_workflow(
        'halttest', halt_workflow
    )
    app.extensions['invenio-workflows'].register_workflow(
        'halttestcond', halt_workflow_conditional
    )

    assert 'halttest' in app.extensions['invenio-workflows'].workflows
    assert 'halttestcond' in app.extensions['invenio-workflows'].workflows

    with app.app_context():
        data = [{'foo': 'bar'}]

        eng = start('halttest', data)
        obj = list(eng.objects)[0]

        assert obj.known_statuses.WAITING == obj.status
        assert WorkflowStatus.HALTED == eng.status

        obj_id = obj.id
        obj.continue_workflow()

        obj = DbWorkflowObject.query.get(obj_id)
        assert obj.known_statuses.COMPLETED == obj.status

        eng = start('halttestcond', data)
        obj = list(eng.objects)[0]

        assert obj.known_statuses.WAITING == obj.status
        assert WorkflowStatus.HALTED == eng.status

        obj_id = obj.id
        obj.continue_workflow()

        obj = DbWorkflowObject.query.get(obj_id)
        assert obj.known_statuses.COMPLETED == obj.status
