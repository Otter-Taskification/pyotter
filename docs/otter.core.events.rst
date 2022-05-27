otter.core.events
=================

.. module:: otter.core.events

.. contents:: Table of Contents
    :depth: 3
    :local:


The ``EventFactory`` class
-----------------------------------

.. autoclass:: otter.core.events.EventFactory
    :members:
    :undoc-members:
    :private-members:
    :special-members: __init__, __iter__


The ``_Event`` abstract base class
-----------------------------------

.. warning::

    It looks like ``update_chunks`` is overridden by ``chunks.py``:

    .. code-block:: python

        if event.is_chunk_switch_event:
            self.log.debug(f"updating chunks")
            yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
        else:
            self.chunk_dict[event.encountering_task_id].append_event(event)

    Check whether this is correct! I suspect this should be:

    .. code-block:: python

        if event.is_chunk_switch_event:
            self.log.debug(f"updating chunk and yielding if complete")
            yield from event.update_chunks(self.chunk_dict, self.chunk_stack)
        else:
            self.log.debug(f"updating chunk")
            event.update_chunks(self.chunk_dict, self.chunk_stack)


.. autoclass:: otter.core.events._Event
    :members:
    :undoc-members:
    :private-members: _event, _additional_attributes
    :special-members: __init__, __getattr__


Mixin classes
-------------

.. .. automodule:: otter.core.events
..    :members: ClassNotImplementedMixin,
..         DefaultUpdateChunksMixin,
..         EnterMixin,
..         LeaveMixin,
..         RegisterTaskDataMixin,
..         ChunkSwitchEventMixin
..    :undoc-members:
..    :show-inheritance:


``ClassNotImplementedMixin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ClassNotImplementedMixin
    :undoc-members:


``DefaultUpdateChunksMixin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: DefaultUpdateChunksMixin
    :undoc-members:


``EnterMixin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: EnterMixin
    :undoc-members:


``LeaveMixin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: LeaveMixin
    :undoc-members:


``RegisterTaskDataMixin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: RegisterTaskDataMixin
    :undoc-members:


``ChunkSwitchEventMixin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ChunkSwitchEventMixin
    :undoc-members:


.. Concrete event classes
.. ----------------------

.. .. automodule:: otter.core.events
..    :exclude-members: _Event,
..         EventFactory,
..         Location,
..         ClassNotImplementedMixin,
..         DefaultUpdateChunksMixin,
..         EnterMixin,
..         LeaveMixin,
..         RegisterTaskDataMixin,
..         ChunkSwitchEventMixin
..    :members:
..    :undoc-members:
..    :show-inheritance:

Non-task event classes
----------------------

``GenericEvent``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: GenericEvent
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: GenericEvent
    :private-bases:
    :top-classes: _Event
    :parts: 1

``Master``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: Master
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: Master
    :private-bases:
    :top-classes: _Event
    :parts: 1

``MasterBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: MasterBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: MasterBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``MasterEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: MasterEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: MasterEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1

``ParallelBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ParallelBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: ParallelBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``ParallelEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ParallelEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: ParallelEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1

``SingleBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: SingleBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: SingleBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``SingleEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: SingleEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: SingleEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1

``Sync``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: Sync
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: Sync
    :private-bases:
    :top-classes: _Event
    :parts: 1

``SyncBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: SyncBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: SyncBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``SyncEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: SyncEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: SyncEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskgroupBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskgroupBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskgroupBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskgroupEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskgroupEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskgroupEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1

``ThreadBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ThreadBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: ThreadBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``ThreadEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ThreadEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: ThreadEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1

``WorkshareBegin``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: WorkshareBegin
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: WorkshareBegin
    :private-bases:
    :top-classes: _Event
    :parts: 1

``WorkshareEnd``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: WorkshareEnd
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: WorkshareEnd
    :private-bases:
    :top-classes: _Event
    :parts: 1


Task-related event classes
--------------------------

``Task``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: Task
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: Task
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskCreate``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskCreate
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskCreate
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskEnter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskEnter
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskEnter
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskLeave``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskLeave
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskLeave
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskSchedule``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskSchedule
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskSchedule
    :private-bases:
    :top-classes: _Event
    :parts: 1

``TaskSwitch``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: TaskSwitch
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: TaskSwitch
    :private-bases:
    :top-classes: _Event
    :parts: 1

``InitialTaskEnter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: InitialTaskEnter
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: InitialTaskEnter
    :private-bases:
    :top-classes: _Event
    :parts: 1

``InitialTaskLeave``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: InitialTaskLeave
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: InitialTaskLeave
    :private-bases:
    :top-classes: _Event
    :parts: 1

``ImplicitTaskEnter``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ImplicitTaskEnter
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: ImplicitTaskEnter
    :private-bases:
    :top-classes: _Event
    :parts: 1

``ImplicitTaskLeave``
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

.. autoclass:: ImplicitTaskLeave
    :members:
    :undoc-members:
    :private-members:
    :show-inheritance:

.. inheritance-diagram:: ImplicitTaskLeave
    :private-bases:
    :top-classes: _Event
    :parts: 1



Task-related class inheritance diagram
--------------------------------------

.. inheritance-diagram:: 
        ImplicitTaskEnter
        ImplicitTaskLeave
        InitialTaskEnter
        InitialTaskLeave
        Task
        TaskCreate
        TaskEnter
        TaskLeave
        TaskSchedule
        TaskSwitch
    :top-classes: _Event,
        ClassNotImplementedMixin,
        DefaultUpdateChunksMixin,
        EnterMixin,
        LeaveMixin,
        RegisterTaskDataMixin,
        ChunkSwitchEventMixin
    :parts: 1
    :private-bases:




The ``Location`` class
---------------------------

.. autoclass:: otter.core.events.Location
    :members:
    :undoc-members:
    :private-members:


Class inheritance diagram
--------------------------

.. inheritance-diagram:: GenericEvent
        ImplicitTaskEnter
        ImplicitTaskLeave
        InitialTaskEnter
        InitialTaskLeave
        Master
        MasterBegin
        MasterEnd
        ParallelBegin
        ParallelEnd
        SingleBegin
        SingleEnd
        Sync
        SyncBegin
        SyncEnd
        Task
        TaskCreate
        TaskEnter
        TaskLeave
        TaskSchedule
        TaskSwitch
        TaskgroupBegin
        TaskgroupEnd
        ThreadBegin
        ThreadEnd
        WorkshareBegin
        WorkshareEnd
    :top-classes: _Event,
        ClassNotImplementedMixin,
        DefaultUpdateChunksMixin,
        EnterMixin,
        LeaveMixin,
        RegisterTaskDataMixin,
        ChunkSwitchEventMixin
    :parts: 1
    :private-bases:


