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

"""Models for BibWorkflow Objects."""

import base64

from collections import Iterable, namedtuple

from datetime import datetime

from flask import current_app

import uuid

from six import iteritems, callable
from six.moves import cPickle

from sqlalchemy import desc
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy_utils.types import ChoiceType, UUIDType

from invenio_db import db

from workflow.engine_db import WorkflowStatus, EnumLabel
from workflow.utils import staticproperty

from .signals import workflow_object_saved
from .proxies import workflows

from .utils import get_func_info


class ObjectStatus(EnumLabel):

    INITIAL = 0
    COMPLETED = 1
    HALTED = 2
    RUNNING = 3
    WAITING = 4
    ERROR = 5

    @staticproperty
    def labels():  # pylint: disable=no-method-argument
        return {
            0: "New",
            1: "Done",
            2: "Need action",
            3: "In process",
            4: "Waiting",
            5: "Error",
        }


class CallbackPosType(db.PickleType):

    def process_bind_param(self, value, dialect):
        if not isinstance(value, Iterable):
            raise TypeError("Task counter must be an iterable!")
        return self.type_impl.process_bind_param(value, dialect)  # pylint: disable=no-member


def _decode(data):
    return cPickle.loads(base64.b64decode(data))


def _encode(data):
    return base64.b64encode(cPickle.dumps(data))


def get_default_data():
    return {}


def get_default_extra_data():
    return {
        "_error_msg": None,
        "_action": None,
        "source": "",
        "_task_history": []
    }

_encoded_default_extra_data = _encode(get_default_extra_data())


_encoded_default_data = _encode(get_default_data())


class Workflow(db.Model):

    """Represents a workflow instance.

    Used by DbWorkflowEngine to store the state of the workflow.
    """

    __tablename__ = "workflows_workflow"

    uuid = db.Column(UUIDType, primary_key=True, nullable=False, default=uuid.uuid4())
    name = db.Column(db.String(255), default="Default workflow",
                     nullable=False)
    created = db.Column(db.DateTime, default=datetime.now, nullable=False)
    modified = db.Column(db.DateTime, default=datetime.now,
                         onupdate=datetime.now, nullable=False)
    id_user = db.Column(db.Integer, default=0, nullable=False)
    _extra_data = db.Column(db.LargeBinary,
                            nullable=False,
                            default=_encoded_default_extra_data)
    status = db.Column(ChoiceType(WorkflowStatus, impl=db.Integer()),
                       default=WorkflowStatus.NEW, nullable=False)
    objects = db.relationship("DbWorkflowObject",
                              backref='workflows_workflow',
                              cascade="all, delete-orphan")

    def get_counter(self, object_status):
        return DbWorkflowObject.query.filter(
            DbWorkflowObject.id_workflow == self.uuid,
            DbWorkflowObject.status == object_status
        ).count()

    @hybrid_property
    def counter_initial(self):
        return self.get_counter(ObjectStatus.INITIAL)

    # Deprecated
    @counter_initial.setter
    def counter_initial(self, value):
        pass

    @hybrid_property
    def counter_halted(self):
        return self.get_counter(ObjectStatus.HALTED)

    # Deprecated
    @counter_halted.setter
    def counter_halted(self, value):
        pass

    @hybrid_property
    def counter_error(self):
        return self.get_counter(ObjectStatus.ERROR)

    # Deprecated
    @counter_error.setter
    def counter_error(self, value):
        pass

    @hybrid_property
    def counter_finished(self):
        return self.get_counter(ObjectStatus.COMPLETED)

    # Deprecated
    @counter_finished.setter
    def counter_finished(self, value):
        pass

    def __getattribute__(self, name):
        """Return `extra_data` user-facing storage representations.

        Initialize the one requested with default content if it is not yet
        loaded.

        Calling :py:func:`.save` is neccessary to reflect any changes made to
        these objects in the model.
        """
        data_getter = {
            'extra_data': Mapping('_extra_data', _encoded_default_extra_data),
        }
        if name in data_getter and name not in self.__dict__:
            mapping = data_getter[name]
            if getattr(self, mapping.db_name) is None:
                # Object has not yet been intialized
                stored_data = mapping.default_x_data
            else:
                stored_data = getattr(self, mapping.db_name)
            setattr(self, name, _decode(stored_data))
        return object.__getattribute__(self, name)

    def __dir__(self):
        """Restore auto-completion for names found via `__getattribute__`."""
        dir_ = dir(type(self)) + list(self.__dict__.keys())
        dir_.extend(('extra_data',))
        return sorted(dir_)

    def __repr__(self):
        """Represent a workflow object."""
        return "<Workflow(name: %s, cre: %s, mod: %s," \
               "id_user: %s, status: %s)>" % \
               (str(self.name),  str(self.created), str(self.modified),
                str(self.id_user), str(self.status))

    @classmethod
    def get(cls, *criteria, **filters):
        """Wrapper to get a specified object.

        A wrapper for the filter and filter_by functions of sqlalchemy.
        Define a dict with which columns should be filtered by which values.

        .. code-block:: python

            Workflow.get(uuid=uuid)
            Workflow.get(Workflow.uuid != uuid)

        The function supports also "hybrid" arguments.

        .. code-block:: python

            Workflow.get(Workflow.module_name != 'i_hate_this_module',
                         user_id=user_id)

        See also SQLAlchemy BaseQuery's filter and filter_by documentation.
        """
        return cls.query.filter(*criteria).filter_by(**filters)

    @classmethod
    def get_status(cls, uuid=None):
        """Return the status of the workflow."""
        return cls.get(Workflow.uuid == uuid).one().status

    @classmethod
    def get_most_recent(cls, *criteria, **filters):
        """Return the most recently modified workflow."""
        most_recent = cls.get(*criteria, **filters). \
            order_by(desc(Workflow.modified)).first()
        if most_recent is None:
            raise NoResultFound
        else:
            return most_recent

    @classmethod
    def get_objects(cls, uuid=None):
        """Return the objects of the workflow."""
        return cls.get(Workflow.uuid == uuid).one().objects

    # Deprecated
    def get_extra_data(self, user_id=0, uuid=None, key=None, getter=None):
        """Get the extra_data for the object.

        Returns a JSON of the column extra_data or
        if any of the other arguments are defined,
        a specific value.

        You can define either the key or the getter function.

        :param key: the key to access the desirable value
        :param getter: callable that takes a dict as param and returns a value
        """
        # extra_data = Workflow.get(Workflow.id_user == self.id_user,
        #                           Workflow.uuid == self.uuid).one()._extra_data

        if key:
            return self.extra_data[key]
        elif callable(getter):
            return getter(self.extra_data)
        elif not key:
            return self.extra_data

    # Deprecated
    def set_extra_data(self, user_id=0, uuid=None,
                       key=None, value=None, setter=None):
        """Replace extra_data.

        Modifies the JSON of the column extra_data or
        if any of the other arguments are defined, a specific value.
        You can define either the key, value or the setter function.

        :param key: the key to access the desirable value
        :param value: the new value
        :param setter: a callable that takes a dict as param and modifies it
        """
        # extra_data = Workflow.get(Workflow.id_user == user_id,
        #                           Workflow.uuid == uuid).one()._extra_data
        if key is not None and value is not None:
            self.extra_data[key] = value
        elif callable(setter):
            setter(self.extra_data)

        # Workflow.get(Workflow.uuid == self.uuid).update(
        #     {'_extra_data': base64.b64encode(cPickle.dumps(extra_data))}
        # )

    @classmethod
    def delete(cls, uuid=None):
        """Delete a workflow."""
        uuid = uuid or cls.uuid
        db.session.delete(cls.get(Workflow.uuid == uuid).first())

    def save(self, status=None):
        """Save object to persistent storage."""
        with db.session.begin_nested():
            self.modified = datetime.now()
            if status is not None:
                self.status = status
            self._extra_data = _encode(self.extra_data)
            db.session.merge(self)
        db.session.commit()



Mapping = namedtuple('Mapping', ['db_name', 'default_x_data'])


class DbWorkflowObject(db.Model):

    """Data model for wrapping data being run in the workflows.

    Main object being passed around in the workflows module
    when using the workflows API.

    It can be instantiated like this:

    .. code-block:: python

        obj = DbWorkflowObject()
        obj.save()

    Or, like this:

    .. code-block:: python

        obj = DbWorkflowObject.create_object()

    DbWorkflowObject provides some handy functions such as:

    .. code-block:: python

        obj.data = "<xml ..... />"
        obj.data == "<xml ..... />"
        obj.extra_data = {"param": value}
        obj.extra_data == {"param": value}

    Then to finally save the object

    .. code-block:: python

        obj.save()

    Now you can for example run it in a workflow:

    .. code-block:: python

        obj.start_workflow("sample_workflow")
    """

    __tablename__ = "workflows_object"

    id = db.Column(db.Integer, primary_key=True)

    # Our internal data column. Default is encoded dict.
    _data = db.Column(db.LargeBinary, nullable=False,
                      default=_encoded_default_data)

    _extra_data = db.Column(db.LargeBinary, nullable=False,
                            default=_encoded_default_extra_data)

    _id_workflow = db.Column(UUIDType,
                             db.ForeignKey("workflows_workflow.uuid", ondelete='CASCADE'),
                             nullable=True, name="id_workflow")

    status = db.Column(ChoiceType(ObjectStatus, impl=db.Integer()),
                       default=ObjectStatus.INITIAL, nullable=False,
                       index=True)

    id_parent = db.Column(db.Integer, db.ForeignKey("workflows_object.id", ondelete='CASCADE'),
                          default=None)

    child_objects = db.relationship("DbWorkflowObject", remote_side=[id_parent])

    created = db.Column(db.DateTime, default=datetime.now, nullable=False)

    modified = db.Column(db.DateTime, default=datetime.now,
                         onupdate=datetime.now, nullable=False)

    data_type = db.Column(db.String(150), default="", nullable=True, index=True)

    uri = db.Column(db.String(500), default="")

    id_user = db.Column(db.Integer, default=0, nullable=False)

    callback_pos = db.Column(CallbackPosType(), default=[])  # ex-task_counter

    workflow = db.relationship(
        Workflow, foreign_keys=[_id_workflow], remote_side=Workflow.uuid,
        post_update=True,
    )

    @hybrid_property
    def id_workflow(self):  # pylint: disable=method-hidden
        """Get id_workflow."""
        return self._id_workflow

    @id_workflow.setter
    def id_workflow(self, value):
        """Set id_workflow."""
        self._id_workflow = str(value) if value else None

    @staticproperty
    def known_statuses():  # pylint: disable=no-method-argument
        return ObjectStatus

    # Deprecated
    @hybrid_property
    def version(self):
        return self.status

    # Deprecated (raise some warning here XXX)
    @version.setter
    def version_setter(self, value):
        pass

    def __getattribute__(self, name):
        """Return `data` and `extra_data` user-facing storage representations.

        Initialize the one requested with default content if it is not yet
        loaded.

        Calling :py:func:`.save` is neccessary to reflect any changes made to
        these objects in the model.
        """
        data_getter = {
            'data': Mapping('_data', _encoded_default_data),
            'extra_data': Mapping('_extra_data', _encoded_default_extra_data),
        }
        if name in data_getter and name not in self.__dict__:
            mapping = data_getter[name]
            if getattr(self, mapping.db_name) is None:
                # Object has not yet been intialized
                stored_data = mapping.default_x_data
            else:
                stored_data = getattr(self, mapping.db_name)
            setattr(self, name, _decode(stored_data))
        return object.__getattribute__(self, name)

    def __dir__(self):
        """Restore auto-completion for names found via `__getattribute__`."""
        dir_ = dir(type(self)) + list(self.__dict__.keys())
        dir_.extend(('data', 'extra_data',))
        return sorted(dir_)

    @property
    def log(self):
        """Access logger object for this instance."""
        return current_app.logger

    # Deprecated
    def get_data(self):
        """Get data saved in the object."""
        return self.data

    # Deprecated
    def set_data(self, value):
        """Save data to the object."""
        self.data = value

    # Deprecated
    def get_extra_data(self):
        """Get extra data saved to the object."""
        return self.extra_data

    # Deprecated
    def set_extra_data(self, value):
        """Save extra data to the object.

        :param value: what you want to replace extra_data with.
        :type value: dict
        """
        self.extra_data = value

    def get_workflow_name(self):
        """Return the workflow name for this object."""
        try:
            if self.id_workflow:
                return Workflow.query.get(self.id_workflow).name
        except AttributeError:
            # Workflow non-existent
            pass
        return

    def get_formatted_data(self, **kwargs):
        """Get the formatted representation for this object."""
        try:
            name = self.get_workflow_name()
            if not name:
                return "Did not find any way to format data."
            workflow_definition = workflows[name]
            formatted_data = workflow_definition.formatter(
                self,
                **kwargs
            )
        except (KeyError, AttributeError) as err:
            # Somehow the workflow or formatter does not exist
            formatted_data = "Error formatting record: {0}".format(err)
            current_app.logger.exception(err)
        return formatted_data

    def __repr__(self):
        """Represent a DbWorkflowObject."""
        return "<DbWorkflowObject(id = %s, id_workflow = %s, " \
               "status = %s, id_parent = %s, created = %s, )" \
               % (str(self.id), str(self.id_workflow), str(self.status),
                  str(self.id_parent), str(self.created))

    def __eq__(self, other):
        """Enable equal operators on DbWorkflowObjects."""
        if isinstance(other, DbWorkflowObject):
            if self._data == other._data and \
                    self._extra_data == other._extra_data and \
                    self.id_workflow == other.id_workflow and \
                    self.status == other.status and \
                    self.id_parent == other.id_parent and \
                    isinstance(self.created, datetime) and \
                    isinstance(self.modified, datetime):
                return True
            else:
                return False
        return NotImplemented

    def __ne__(self, other):
        """Enable equal operators on DbWorkflowObjects."""
        return not self.__eq__(other)

    def set_action(self, action, message):
        """Set the action to be taken for this object.

        Assign an special "action" to this object to be taken
        in consideration in Holding Pen. The widget is referred to
        by a string with the filename minus extension.

        A message is also needed to tell the user the action
        required in a textual way.

        :param action: name of the action to add (i.e. "approval")
        :type action: string

        :param message: message to show to the user
        :type message: string
        """
        self.extra_data["_action"] = action
        self.extra_data["_message"] = message

    def get_action(self):
        """Retrieve the currently assigned action, if any.

        :return: name of action assigned as string, or None
        """
        return self.extra_data.get("_action")

    def get_action_message(self):
        """Retrieve the currently assigned widget, if any."""
        return self.extra_data.get("_message")

    def remove_action(self):
        """Remove the currently assigned action."""
        extra_data = self.extra_data
        extra_data["_action"] = None
        extra_data["_message"] = ""
        if "_widget" in extra_data:
            del extra_data["_widget"]
        self.set_extra_data(extra_data)

    def start_workflow(self, workflow_name, delayed=False, **kwargs):
        """Run the workflow specified on the object.

        Will start workflows execution for the object using
        :py:func:`.api.start` (or :py:func:`.api.start_delayed`
        if `delayed=True`).


        :param workflow_name: name of workflow to run
        :type workflow_name: str

        :param delayed: should the workflow run asynchronously?
        :type delayed: bool

        :return: DbWorkflowEngine (or AsynchronousResultWrapper).
        """
        # FIXME: Move from invenio
        if delayed:
            from .api import start_delayed as start_func
            self.save()
        else:
            from .api import start as start_func
        return start_func(workflow_name, data=[self], **kwargs)

    def continue_workflow(self, start_point="continue_next",
                          delayed=False, **kwargs):
        """Continue the workflow for this object.

        Will continue a previous execution for the object using
        :py:func:`.api.continue_oid` (or :py:func:`.api.continue_oid_delayed`
        if `delayed=True`).

        The parameter `start_point` allows you to specify the point of where
        the workflow shall continue:

        * restart_prev: will restart from the previous task

        * continue_next: will continue to the next task

        * restart_task: will restart the current task

        :param start_point: where should the workflow start from?
        :type start_point: str

        :param delayed: should the workflow run asynchronously?
        :type delayed: bool

        :return: DbWorkflowEngine (or AsynchronousResultWrapper).
        """
        from workflow.errors import WorkflowAPIError

        self.save()
        if not self.id_workflow:
            raise WorkflowAPIError("No workflow associated with object: %r"
                                   % (repr(self),))
        if delayed:
            from .api import continue_oid_delayed as continue_func
        else:
            from .api import continue_oid as continue_func
        return continue_func(self.id, start_point, **kwargs)

    # Deprecated
    def get_current_task(self):
        """Return the current task from the workflow engine for this object."""
        return self.callback_pos

    def get_current_task_info(self):
        """Return dictionary of current task function info for this object."""
        name = self.get_workflow_name()
        if not name:
            return

        current_task = workflows[name].workflow
        for step in self.callback_pos:
            current_task = current_task[step]
            if callable(current_task):
                return get_func_info(current_task)

    def copy(self, other):
        """Copy data and metadata except id and id_workflow."""
        for attr in ('status', 'id_parent', 'created',
                     'modified', 'status', 'data_type', 'uri'):
            setattr(self, attr, getattr(other, attr))
        setattr(self, 'data', other.data)
        setattr(self, 'extra_data', other.extra_data)
        self.save()

    def save(self, status=None, callback_pos=None, id_workflow=None):
        """Save object to persistent storage."""
        with db.session.begin_nested():
            if callback_pos is not None:
                self.callback_pos = callback_pos  # Used by admins
            self.log.debug("Current callback pos: %s" % (self.callback_pos,))
            self._data = _encode(self.data)
            self._extra_data = _encode(self.extra_data)

            self.modified = datetime.now()
            if status is not None:
                self.status = status
            if id_workflow is not None:
                self.id_workflow = id_workflow
            db.session.merge(self)
            if self.id is not None:
                # Because the logger will save to the DB so it NEEDS self.id to be
                # not None
                self.log.debug("Saved object: %s" % (self.id or "new",))
        db.session.commit()
        workflow_object_saved.send(self)

    @classmethod
    def get(cls, *criteria, **filters):
        """Wrapper of SQLAlchemy to get a DbWorkflowObject.

        A wrapper for the filter and filter_by functions of SQLAlchemy.
        Define a dict with which columns should be filtered by which values.

        .. code-block:: python

            Workflow.get(uuid=uuid)
            Workflow.get(Workflow.uuid != uuid)

        The function supports also "hybrid" arguments.

        .. code-block:: python

            Workflow.get(Workflow.module_name != 'i_hate_this_module',
                         user_id=user_id)

        See also SQLAlchemy BaseQuery's filter and filter_by documentation.
        """
        return cls.query.filter(*criteria).filter_by(**filters)

    @classmethod
    def delete(cls, oid):
        """Delete a DbWorkflowObject."""
        if isinstance(oid, DbWorkflowObject):
            db.session.delete(oid)
        else:
            db.session.delete(
                DbWorkflowObject.get(DbWorkflowObject.id == oid).first())

    @classmethod
    def create_object(cls, **kwargs):
        """Create a new Workflow Object with given content."""
        obj = DbWorkflowObject(**kwargs)
        with db.session.begin_nested():
            db.session.add(obj)
        db.session.commit()
        return obj

    @classmethod
    def create_object_revision(cls, old_obj, status, **kwargs):
        """Create a Workflow Object copy with customized values."""
        # Create new object and copy it
        obj = DbWorkflowObject(**kwargs)
        obj.copy(old_obj)

        # Overwrite some changes

        obj.status = status
        obj.created = datetime.now()
        obj.modified = datetime.now()
        for key, value in iteritems(kwargs):
            setattr(obj, key, value)
        db.session.add(obj)
        return obj


__all__ = ('Workflow', 'DbWorkflowObject')
