# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2013, 2014, 2015 CERN.
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

"""Unit tests for workflows models."""

from __future__ import absolute_import

from uuid import uuid1

from invenio_db import db

from invenio_workflows import DbWorkflowObject, Workflow


def test_db(app):
    assert 'workflows_object' in db.metadata.tables
    assert 'workflows_workflow' in db.metadata.tables

    with app.app_context():
        workflow = Workflow(name='demo_workflow', uuid=uuid1(),
                            id_user=0)
        workflow.save()
        bibworkflowobject = DbWorkflowObject(workflow=workflow)
        bibworkflowobject.save()
        db.session.commit()

        bwo_id = bibworkflowobject.id
        # delete workflow
        Workflow.delete(workflow.uuid)

        # assert bibworkflowobject is deleted
        assert not (
            db.session.query(
                DbWorkflowObject.query.filter(
                    DbWorkflowObject.id == bwo_id).exists()).scalar())

        workflow = Workflow(name='demo_workflow', uuid=uuid1(),
                            id_user=0)
        workflow.save()
        w_uuid = workflow.uuid
        bibworkflowobject = DbWorkflowObject(workflow=workflow)
        bibworkflowobject.save()
        db.session.commit()

        # delete bibworkflowobject
        DbWorkflowObject.query.filter(
            DbWorkflowObject.id == bibworkflowobject.id
        ).delete()
        db.session.commit()

        # assert workflow is not deleted
        assert (
            db.session.query(
                Workflow.query.filter(
                    Workflow.uuid == w_uuid).exists()).scalar())
