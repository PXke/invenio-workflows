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

import pytest

from flask import Flask


from demo_package.workflows.demo_workflow import demo_workflow

from invenio_db import db

from invenio_workflows import InvenioWorkflows, start

from workflow.engine_db import WorkflowStatus


@pytest.fixture
def halt_workflow(app):
    def halt_engine(obj, eng):
        return eng.halt("Test")

    class HaltTest(object):
        workflow = [halt_engine]

    return HaltTest


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


def test_api(app):
    """Test api."""
    with app.app_context():
        pass


def test_halt(app, halt_workflow):
    """Test halt task."""
    app.extensions['invenio-workflows'].register_workflow(
        'halttest', halt_workflow
    )

    assert 'halttest' in app.extensions['invenio-workflows'].workflows

    data = [{'foo': 'bar'}]

    with app.app_context():
        db.create_all()

    with app.app_context():
        eng = start('halttest', data)
        obj = list(eng.objects)[0]

        assert obj.known_statuses.WAITING == obj.status
        assert WorkflowStatus.HALTED == eng.status

    with app.app_context():
        db.drop_all()


def test_continue_object(app, halt_workflow):
    """Test continue object task."""
    from invenio_workflows.models import DbWorkflowObject

    app.extensions['invenio-workflows'].register_workflow(
        'halttest', halt_workflow
    )

    data = [{'foo': 'bar'}]

    with app.app_context():
        db.create_all()

    with app.app_context():
        eng = start('halttest', data)
        obj = list(eng.objects)[0]

        assert obj.known_statuses.WAITING == obj.status
        assert WorkflowStatus.HALTED == eng.status

        obj_id = obj.id
        obj.continue_workflow(delayed=True)

        obj = DbWorkflowObject.query.get(obj_id)
        assert obj.known_statuses.COMPLETED == obj.status

    with app.app_context():
        db.drop_all()
