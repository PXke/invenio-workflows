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

"""The workflow engine extension of GenericWorkflowEngine."""

from __future__ import absolute_import

from copy import deepcopy
from uuid import uuid1 as new_uuid

from flask import current_app

from invenio_db import db

from workflow.engine import (
    ActionMapper,
    Break,
    Continue,
    TransitionActions,
)
from workflow.engine_db import (
    DbProcessingFactory,
    DbTransitionAction,
    DbWorkflowEngine,
    WorkflowStatus,
)
from workflow.errors import WorkflowDefinitionError
from workflow.utils import staticproperty

from .errors import WaitProcessing
from .models import (
    DbWorkflowObject,
    ObjectStatus,
    Workflow,
    get_default_extra_data,
)
from .utils import get_task_history


class BibWorkflowEngine(DbWorkflowEngine):

    """Special engine for Invenio.

    The reason why base64 is used throughout this class is due to a bug in
    CPython pickle streams which sometimes contain non-ASCII characters.
    Because of this it is impossible to correctly use json on such data without
    base64 encoding it first.
    """

    def __init__(self, db_obj, **extra_data):
        """Special handling of instantiation of engine."""
        # Super's __init__ clears extra_data, which we override to be
        # db_obj.extra_data. We work around this by temporarily storing it
        # elsewhere.
        _extra_data = deepcopy(db_obj.extra_data)
        super(BibWorkflowEngine, self).__init__(db_obj)
        self.extra_data = _extra_data

        self.extra_data.update(extra_data)
        self.set_workflow_by_name(self.db_obj.name)

    def __dir__(self):
        """Restore auto-completion for names found via `__getattr__`."""
        dir_ = dir(type(self)) + list(self.__dict__.keys())
        dir_.extend(('extra_data',))
        return sorted(dir_)

    def __getattribute__(self, name):
        """Return `extra_data` user-facing storage representations."""
        if name == 'extra_data':
            return self.db_obj.extra_data
        else:
            return object.__getattribute__(self, name)

    def __setattr__(self, name, val):
        """Set `extra_data` user-facing storage representations."""
        if name == 'extra_data':
            self.db_obj.extra_data = val
        self.__dict__[name] = val

    @classmethod
    def with_name(cls, name, id_user=0, **extra_data):
        """Instantiate a DbWorkflowEngine given a name or UUID.

        :param name: name of workflow to run.
        :type name: str

        :param id_user: id of user to associate with workflow
        :type id_user: int

        :param module_name: label used to query groups of workflows.
        :type module_name: str
        """
        db_obj = Workflow(
            name=name,
            id_user=id_user,
            uuid=new_uuid()
        )
        return cls(db_obj, **extra_data)

    @classmethod
    def from_uuid(cls, uuid, **extra_data):
        """Load a workflow from the database given a UUID.

        :param uuid: pass a uuid to an existing workflow.
        :type uuid: str
        """
        db_obj = Workflow.get(Workflow.uuid == uuid).first()
        if db_obj is None:
            raise LookupError(
                "No workflow with UUID {} was found".format(uuid)
            )
        return cls(db_obj, **extra_data)

    @property
    def db(self):
        """Return SQLAlchemy db."""
        return db

    @staticproperty
    def object_status():  # pylint: disable=no-method-argument
        return ObjectStatus

    @staticproperty
    def processing_factory():  # pylint: disable=no-method-argument
        """Provide a proccessing factory."""
        return InvProcessingFactory

    @property
    def uuid(self):
        """Return the uuid."""
        return self.db_obj.uuid

    @property
    def id_user(self):
        """Return the user id."""
        return self.db_obj.id_user

    def wait(self, msg=""):
        """Halt the workflow (stop also any parent `wfe`).

        Halts the currently running workflow by raising WaitProcessing.

        :param msg: message explaining the reason for halting.
        :type msg: str

        :raises: WaitProcessing
        """
        raise WaitProcessing(message=msg)

    def continue_object(self, workflow_object, restart_point='restart_task',
                        task_offset=1, stop_on_halt=False):
        """Continue workflow for one given object from "restart_point".

        :param object:
        :param stop_on_halt:
        :param restart_point: can be one of:
            * restart_prev: will restart from the previous task
            * continue_next: will continue to the next task
            * restart_task: will restart the current task

        You can use stop_on_error to raise exception's and stop the processing.
        Use stop_on_halt to stop processing the workflow if HaltProcessing is
        raised.
        """
        translate = {
            'restart_task': 'current',
            'continue_next': 'next',
            'restart_prev': 'prev',
        }
        self.state.callback_pos = workflow_object.get_current_task() or [0]
        self.restart(task=translate[restart_point], obj='first',
                     objects=[workflow_object], stop_on_halt=stop_on_halt)

    def init_logger(self):
        """Return the appropriate logger instance."""
        return current_app.logger

    @property
    def has_completed(self):
        """Return True if workflow is fully completed."""
        objects_in_db = DbWorkflowObject.query.filter(
            DbWorkflowObject.id_workflow == self.uuid,
            DbWorkflowObject.id_parent == None,
        ).filter(DbWorkflowObject.status.in_([
            DbWorkflowObject.known_statuses.COMPLETED
        ])).count()
        return objects_in_db == len(list(self.objects))

    def set_workflow_by_name(self, workflow_name):
        """Configure the workflow to run by the name of this one.

        Allows the modification of the workflow that the engine will run
        by looking in the registry the name passed in parameter.

        :param workflow_name: name of the workflow.
        :type workflow_name: str
        """
        from .proxies import workflows
        if workflow_name not in workflows:
            # No workflow with that name exists
            raise WorkflowDefinitionError("Workflow '%s' does not exist"
                                          % (workflow_name,),
                                          workflow_name=workflow_name)
        self.workflow_definition = workflows[workflow_name]
        self.setWorkflow(self.workflow_definition.workflow)

    def get_default_data_type(self):
        """Return default data type from workflow definition."""
        return getattr(self.workflow_definition, "object_type", "")

    def reset_extra_data(self):
        """Reset extra data to defaults."""
        self.db_obj.extra_data = get_default_extra_data()

    def __repr__(self):
        """Allow to represent the BibWorkflowEngine."""
        return "<BibWorkflow_engine(%s)>" % (self.name,)

    def __str__(self, log=False):
        """Allow to print the BibWorkflowEngine."""
        return """-------------------------------
BibWorkflowEngine
-------------------------------
    %s
-------------------------------
""" % (self.db_obj.__str__(),)


class InvActionMapper(ActionMapper):

    @staticmethod
    def before_each_callback(eng, callback_func, obj):
        """Action to take before every WF callback."""
        eng.log.debug("Executing callback %s" % (repr(callback_func),))

    @staticmethod
    def after_each_callback(eng, callback_func, obj):
        """Action to take after every WF callback."""
        obj.callback_pos = eng.state.callback_pos
        obj.extra_data["_last_task_name"] = callback_func.func_name
        task_history = get_task_history(callback_func)
        if "_task_history" not in obj.extra_data:
            obj.extra_data["_task_history"] = [task_history]
        else:
            obj.extra_data["_task_history"].append(task_history)


class InvProcessingFactory(DbProcessingFactory):

    @staticproperty
    def transition_exception_mapper():  # pylint: disable=no-method-argument
        """Define our for handling transition exceptions."""
        return InvTransitionAction

    @staticproperty
    def action_mapper():  # pylint: disable=no-method-argument
        """Set a mapper for actions while processing."""
        return InvActionMapper

    @staticmethod
    def before_object(eng, objects, obj):
        """Action to take before the processing of an object begins."""
        obj.reset_error_message()
        super(InvProcessingFactory, InvProcessingFactory).before_object(
            eng, objects, obj
        )


class InvTransitionAction(DbTransitionAction):

    @staticmethod
    def WaitProcessing(obj, eng, callbacks, exc_info):
        """Take actions when WaitProcessing is raised.

        ..note::
            We're essentially doing HaltProcessing, plus `obj.set_action` and
            object status `WAITING` instead of `HALTED`.
        """
        e = exc_info[1]
        obj.set_action(e.action, e.message)
        obj.save(status=eng.object_status.WAITING,
                 callback_pos=eng.state.callback_pos,
                 id_workflow=eng.uuid)
        eng.save(WorkflowStatus.HALTED)
        eng.log.warning("Workflow '%s' halted at task %s with message: %s",
                        eng.name, eng.current_taskname or "Unknown", e.message)
        TransitionActions.HaltProcessing(obj, eng, callbacks, exc_info)

    @staticmethod
    def HaltProcessing(obj, eng, callbacks, exc_info):
        e = exc_info[1]
        if e.action:
            obj.set_action(e.action, e.message)
            obj.save(status=eng.object_status.HALTED,
                     callback_pos=eng.state.callback_pos,
                     id_workflow=eng.uuid)
            eng.save(WorkflowStatus.HALTED)
            TransitionActions.HaltProcessing(obj, eng, callbacks, exc_info)
            eng.log.warning(
                "Workflow '%s' waiting at task %s with message: %s",
                eng.name, eng.current_taskname or "Unknown", e.message
            )
        else:
            InvTransitionAction.WaitProcessing(obj, eng, callbacks, exc_info)

    @staticmethod
    def StopProcessing(obj, eng, callbacks, exc_info):
        """Gracefully stop the execution of the engine."""
        msg = "Processing was stopped for object: {0}".format(obj.id)
        eng.log.debug(msg)
        raise Break

    @staticmethod
    def SkipToken(obj, eng, callbacks, exc_info):
        """Action to take when SkipToken is raised."""
        msg = "Skipped running this object: {0}".format(obj.id)
        eng.log.debug(msg)
        raise Continue

    @staticmethod
    def AbortProcessing(obj, eng, callbacks, exc_info):
        """Action to take when AbortProcessing is raised."""
        msg = "Processing was aborted for object: {0}".format(obj.id)
        eng.log.debug(msg)
        raise Break
