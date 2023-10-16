"""Extensions to otf2"""

from typing import Iterable, Dict, Tuple, Any
from contextlib import contextmanager

from otf2 import events
from otf2.attribute_list import AttributeList

import _otf2

_otf2_version = (_otf2.VERSION_MAJOR, _otf2.VERSION_MINOR)

class SeekingEventReader:
    """
    This class must mirror otf2.event_reader.BufferedEventReader to allow using
    _otf2.EvtReader_Seek

    Functionality to mirror:
        - setting up callbacks
        - loading handles
    """

    def __init__(self, definitions):
        self.definitions = definitions

    def seek_events(self, positions: Iterable[Tuple[int, int]], readers: Dict[int, Any]):
        with self._callbacks(readers):
            for location_ref, pos in positions:

                # get the local event reader for the given location
                reader = readers[location_ref]

                # seek to the position of the requested event
                _otf2.EvtReader_Seek(reader, pos)

                # read and yield the location+event
                read_events = _otf2.EvtReader_ReadEvents(reader, 1)
                if read_events < 1:
                    return
                else:
                    yield self._location_event

    def _set_location_event(self, event_type, location_ref, time, event_position, _, attribute_list, *args):
        event = event_type._construct(self.definitions, time, *args)
        event.attributes = AttributeList._construct(self.definitions, attribute_list)
        location = self.definitions.locations[location_ref]
        self._location_event = (location, event)

    @contextmanager
    def _callbacks(self, readers: Dict[int, Any]):
        reader_callbacks = _otf2.EvtReaderCallbacks_New()
        self._set_event_reader_callbacks(reader_callbacks)
        for evt_reader in readers.values():
            _otf2.EvtReader_SetCallbacks(evt_reader, reader_callbacks, None)
        _otf2.EvtReaderCallbacks_Delete(reader_callbacks)
        yield

    def _set_event_reader_callbacks(self, cbs):
        _otf2.EvtReaderCallbacks_SetBufferFlushCallback(cbs, self._buffer_flush)
        _otf2.EvtReaderCallbacks_SetMeasurementOnOffCallback(cbs, self._measurement_on_off)
        _otf2.EvtReaderCallbacks_SetEnterCallback(cbs, self._enter)
        _otf2.EvtReaderCallbacks_SetLeaveCallback(cbs, self._leave)
        _otf2.EvtReaderCallbacks_SetMpiSendCallback(cbs, self._mpi_send)
        _otf2.EvtReaderCallbacks_SetMpiIsendCallback(cbs, self._mpi_isend)
        _otf2.EvtReaderCallbacks_SetMpiIsendCompleteCallback(cbs, self._mpi_isend_complete)
        _otf2.EvtReaderCallbacks_SetMpiIrecvRequestCallback(cbs, self._mpi_irecv_request)
        _otf2.EvtReaderCallbacks_SetMpiRecvCallback(cbs, self._mpi_recv)
        _otf2.EvtReaderCallbacks_SetMpiIrecvCallback(cbs, self._mpi_irecv)
        _otf2.EvtReaderCallbacks_SetMpiRequestTestCallback(cbs, self._mpi_request_test)
        _otf2.EvtReaderCallbacks_SetMpiRequestCancelledCallback(cbs, self._mpi_request_cancelled)
        _otf2.EvtReaderCallbacks_SetMpiCollectiveBeginCallback(cbs, self._mpi_collective_begin)
        _otf2.EvtReaderCallbacks_SetMpiCollectiveEndCallback(cbs, self._mpi_collective_end)
        _otf2.EvtReaderCallbacks_SetOmpForkCallback(cbs, self._omp_fork)
        _otf2.EvtReaderCallbacks_SetOmpJoinCallback(cbs, self._omp_join)
        _otf2.EvtReaderCallbacks_SetOmpAcquireLockCallback(cbs, self._omp_acquire_lock)
        _otf2.EvtReaderCallbacks_SetOmpReleaseLockCallback(cbs, self._omp_release_lock)
        _otf2.EvtReaderCallbacks_SetOmpTaskCreateCallback(cbs, self._omp_task_create)
        _otf2.EvtReaderCallbacks_SetOmpTaskSwitchCallback(cbs, self._omp_task_switch)
        _otf2.EvtReaderCallbacks_SetOmpTaskCompleteCallback(cbs, self._omp_task_complete)
        _otf2.EvtReaderCallbacks_SetMetricCallback(cbs, self._metric)
        _otf2.EvtReaderCallbacks_SetParameterStringCallback(cbs, self._parameter_string)
        _otf2.EvtReaderCallbacks_SetParameterIntCallback(cbs, self._parameter_int)
        _otf2.EvtReaderCallbacks_SetParameterUnsignedIntCallback(cbs, self._parameter_unsigned_int)
        _otf2.EvtReaderCallbacks_SetRmaWinCreateCallback(cbs, self._rma_win_create)
        _otf2.EvtReaderCallbacks_SetRmaWinDestroyCallback(cbs, self._rma_win_destroy)
        _otf2.EvtReaderCallbacks_SetRmaCollectiveBeginCallback(cbs, self._rma_collective_begin)
        _otf2.EvtReaderCallbacks_SetRmaCollectiveEndCallback(cbs, self._rma_collective_end)
        _otf2.EvtReaderCallbacks_SetRmaGroupSyncCallback(cbs, self._rma_group_sync)
        _otf2.EvtReaderCallbacks_SetRmaRequestLockCallback(cbs, self._rma_request_lock)
        _otf2.EvtReaderCallbacks_SetRmaAcquireLockCallback(cbs, self._rma_acquire_lock)
        _otf2.EvtReaderCallbacks_SetRmaTryLockCallback(cbs, self._rma_try_lock)
        _otf2.EvtReaderCallbacks_SetRmaReleaseLockCallback(cbs, self._rma_release_lock)
        _otf2.EvtReaderCallbacks_SetRmaSyncCallback(cbs, self._rma_sync)
        _otf2.EvtReaderCallbacks_SetRmaWaitChangeCallback(cbs, self._rma_wait_change)
        _otf2.EvtReaderCallbacks_SetRmaPutCallback(cbs, self._rma_put)
        _otf2.EvtReaderCallbacks_SetRmaGetCallback(cbs, self._rma_get)
        _otf2.EvtReaderCallbacks_SetRmaAtomicCallback(cbs, self._rma_atomic)
        _otf2.EvtReaderCallbacks_SetRmaOpCompleteBlockingCallback(cbs, self._rma_op_complete_blocking)
        _otf2.EvtReaderCallbacks_SetRmaOpCompleteNonBlockingCallback(cbs, self._rma_op_complete_non_blocking)
        _otf2.EvtReaderCallbacks_SetRmaOpTestCallback(cbs, self._rma_op_test)
        _otf2.EvtReaderCallbacks_SetRmaOpCompleteRemoteCallback(cbs, self._rma_op_complete_remote)
        _otf2.EvtReaderCallbacks_SetThreadForkCallback(cbs, self._thread_fork)
        _otf2.EvtReaderCallbacks_SetThreadJoinCallback(cbs, self._thread_join)
        _otf2.EvtReaderCallbacks_SetThreadTeamBeginCallback(cbs, self._thread_team_begin)
        _otf2.EvtReaderCallbacks_SetThreadTeamEndCallback(cbs, self._thread_team_end)
        _otf2.EvtReaderCallbacks_SetThreadAcquireLockCallback(cbs, self._thread_acquire_lock)
        _otf2.EvtReaderCallbacks_SetThreadReleaseLockCallback(cbs, self._thread_release_lock)
        _otf2.EvtReaderCallbacks_SetThreadTaskCreateCallback(cbs, self._thread_task_create)
        _otf2.EvtReaderCallbacks_SetThreadTaskSwitchCallback(cbs, self._thread_task_switch)
        _otf2.EvtReaderCallbacks_SetThreadTaskCompleteCallback(cbs, self._thread_task_complete)
        _otf2.EvtReaderCallbacks_SetThreadCreateCallback(cbs, self._thread_create)
        _otf2.EvtReaderCallbacks_SetThreadBeginCallback(cbs, self._thread_begin)
        _otf2.EvtReaderCallbacks_SetThreadWaitCallback(cbs, self._thread_wait)
        _otf2.EvtReaderCallbacks_SetThreadEndCallback(cbs, self._thread_end)
        _otf2.EvtReaderCallbacks_SetCallingContextEnterCallback(cbs, self._calling_context_enter)
        _otf2.EvtReaderCallbacks_SetCallingContextLeaveCallback(cbs, self._calling_context_leave)
        _otf2.EvtReaderCallbacks_SetCallingContextSampleCallback(cbs, self._calling_context_sample)
        _otf2.EvtReaderCallbacks_SetIoCreateHandleCallback(cbs, self._io_create_handle)
        _otf2.EvtReaderCallbacks_SetIoDestroyHandleCallback(cbs, self._io_destroy_handle)
        _otf2.EvtReaderCallbacks_SetIoDuplicateHandleCallback(cbs, self._io_duplicate_handle)
        _otf2.EvtReaderCallbacks_SetIoSeekCallback(cbs, self._io_seek)
        _otf2.EvtReaderCallbacks_SetIoChangeStatusFlagsCallback(cbs, self._io_change_status_flags)
        _otf2.EvtReaderCallbacks_SetIoDeleteFileCallback(cbs, self._io_delete_file)
        _otf2.EvtReaderCallbacks_SetIoOperationBeginCallback(cbs, self._io_operation_begin)
        _otf2.EvtReaderCallbacks_SetIoOperationTestCallback(cbs, self._io_operation_test)
        _otf2.EvtReaderCallbacks_SetIoOperationIssuedCallback(cbs, self._io_operation_issued)
        _otf2.EvtReaderCallbacks_SetIoOperationCompleteCallback(cbs, self._io_operation_complete)
        _otf2.EvtReaderCallbacks_SetIoOperationCancelledCallback(cbs, self._io_operation_cancelled)
        _otf2.EvtReaderCallbacks_SetIoAcquireLockCallback(cbs, self._io_acquire_lock)
        _otf2.EvtReaderCallbacks_SetIoReleaseLockCallback(cbs, self._io_release_lock)
        _otf2.EvtReaderCallbacks_SetIoTryLockCallback(cbs, self._io_try_lock)
        _otf2.EvtReaderCallbacks_SetProgramBeginCallback(cbs, self._program_begin)
        _otf2.EvtReaderCallbacks_SetProgramEndCallback(cbs, self._program_end)
        if _otf2_version >= (3, 0):
            _otf2.EvtReaderCallbacks_SetNonBlockingCollectiveRequestCallback(cbs, self._non_blocking_collective_request)
            _otf2.EvtReaderCallbacks_SetNonBlockingCollectiveCompleteCallback(cbs, self._non_blocking_collective_complete)
            _otf2.EvtReaderCallbacks_SetCommCreateCallback(cbs, self._comm_create)
            _otf2.EvtReaderCallbacks_SetCommDestroyCallback(cbs, self._comm_destroy)

    def _buffer_flush(self, *args):
        self._set_location_event(events.BufferFlush, *args)

    def _measurement_on_off(self, *args):
        self._set_location_event(events.MeasurementOnOff, *args)

    def _enter(self, *args):
        self._set_location_event(events.Enter, *args)

    def _leave(self, *args):
        self._set_location_event(events.Leave, *args)

    def _mpi_send(self, *args):
        self._set_location_event(events.MpiSend, *args)

    def _mpi_isend(self, *args):
        self._set_location_event(events.MpiIsend, *args)

    def _mpi_isend_complete(self, *args):
        self._set_location_event(events.MpiIsendComplete, *args)

    def _mpi_irecv_request(self, *args):
        self._set_location_event(events.MpiIrecvRequest, *args)

    def _mpi_recv(self, *args):
        self._set_location_event(events.MpiRecv, *args)

    def _mpi_irecv(self, *args):
        self._set_location_event(events.MpiIrecv, *args)

    def _mpi_request_test(self, *args):
        self._set_location_event(events.MpiRequestTest, *args)

    def _mpi_request_cancelled(self, *args):
        self._set_location_event(events.MpiRequestCancelled, *args)

    def _mpi_collective_begin(self, *args):
        self._set_location_event(events.MpiCollectiveBegin, *args)

    def _mpi_collective_end(self, *args):
        self._set_location_event(events.MpiCollectiveEnd, *args)

    def _omp_fork(self, *args):
        self._set_location_event(events.OmpFork, *args)

    def _omp_join(self, *args):
        self._set_location_event(events.OmpJoin, *args)

    def _omp_acquire_lock(self, *args):
        self._set_location_event(events.OmpAcquireLock, *args)

    def _omp_release_lock(self, *args):
        self._set_location_event(events.OmpReleaseLock, *args)

    def _omp_task_create(self, *args):
        self._set_location_event(events.OmpTaskCreate, *args)

    def _omp_task_switch(self, *args):
        self._set_location_event(events.OmpTaskSwitch, *args)

    def _omp_task_complete(self, *args):
        self._set_location_event(events.OmpTaskComplete, *args)

    def _metric(self, *args):
        self._set_location_event(events.Metric, *args)

    def _parameter_string(self, *args):
        self._set_location_event(events.ParameterString, *args)

    def _parameter_int(self, *args):
        self._set_location_event(events.ParameterInt, *args)

    def _parameter_unsigned_int(self, *args):
        self._set_location_event(events.ParameterUnsignedInt, *args)

    def _rma_win_create(self, *args):
        self._set_location_event(events.RmaWinCreate, *args)

    def _rma_win_destroy(self, *args):
        self._set_location_event(events.RmaWinDestroy, *args)

    def _rma_collective_begin(self, *args):
        self._set_location_event(events.RmaCollectiveBegin, *args)

    def _rma_collective_end(self, *args):
        self._set_location_event(events.RmaCollectiveEnd, *args)

    def _rma_group_sync(self, *args):
        self._set_location_event(events.RmaGroupSync, *args)

    def _rma_request_lock(self, *args):
        self._set_location_event(events.RmaRequestLock, *args)

    def _rma_acquire_lock(self, *args):
        self._set_location_event(events.RmaAcquireLock, *args)

    def _rma_try_lock(self, *args):
        self._set_location_event(events.RmaTryLock, *args)

    def _rma_release_lock(self, *args):
        self._set_location_event(events.RmaReleaseLock, *args)

    def _rma_sync(self, *args):
        self._set_location_event(events.RmaSync, *args)

    def _rma_wait_change(self, *args):
        self._set_location_event(events.RmaWaitChange, *args)

    def _rma_put(self, *args):
        self._set_location_event(events.RmaPut, *args)

    def _rma_get(self, *args):
        self._set_location_event(events.RmaGet, *args)

    def _rma_atomic(self, *args):
        self._set_location_event(events.RmaAtomic, *args)

    def _rma_op_complete_blocking(self, *args):
        self._set_location_event(events.RmaOpCompleteBlocking, *args)

    def _rma_op_complete_non_blocking(self, *args):
        self._set_location_event(events.RmaOpCompleteNonBlocking, *args)

    def _rma_op_test(self, *args):
        self._set_location_event(events.RmaOpTest, *args)

    def _rma_op_complete_remote(self, *args):
        self._set_location_event(events.RmaOpCompleteRemote, *args)

    def _thread_fork(self, *args):
        self._set_location_event(events.ThreadFork, *args)

    def _thread_join(self, *args):
        self._set_location_event(events.ThreadJoin, *args)

    def _thread_team_begin(self, *args):
        self._set_location_event(events.ThreadTeamBegin, *args)

    def _thread_team_end(self, *args):
        self._set_location_event(events.ThreadTeamEnd, *args)

    def _thread_acquire_lock(self, *args):
        self._set_location_event(events.ThreadAcquireLock, *args)

    def _thread_release_lock(self, *args):
        self._set_location_event(events.ThreadReleaseLock, *args)

    def _thread_task_create(self, *args):
        self._set_location_event(events.ThreadTaskCreate, *args)

    def _thread_task_switch(self, *args):
        self._set_location_event(events.ThreadTaskSwitch, *args)

    def _thread_task_complete(self, *args):
        self._set_location_event(events.ThreadTaskComplete, *args)

    def _thread_create(self, *args):
        self._set_location_event(events.ThreadCreate, *args)

    def _thread_begin(self, *args):
        self._set_location_event(events.ThreadBegin, *args)

    def _thread_wait(self, *args):
        self._set_location_event(events.ThreadWait, *args)

    def _thread_end(self, *args):
        self._set_location_event(events.ThreadEnd, *args)

    def _calling_context_enter(self, *args):
        self._set_location_event(events.CallingContextEnter, *args)

    def _calling_context_leave(self, *args):
        self._set_location_event(events.CallingContextLeave, *args)

    def _calling_context_sample(self, *args):
        self._set_location_event(events.CallingContextSample, *args)

    def _io_create_handle(self, *args):
        self._set_location_event(events.IoCreateHandle, *args)

    def _io_destroy_handle(self, *args):
        self._set_location_event(events.IoDestroyHandle, *args)

    def _io_duplicate_handle(self, *args):
        self._set_location_event(events.IoDuplicateHandle, *args)

    def _io_seek(self, *args):
        self._set_location_event(events.IoSeek, *args)

    def _io_change_status_flags(self, *args):
        self._set_location_event(events.IoChangeStatusFlags, *args)

    def _io_delete_file(self, *args):
        self._set_location_event(events.IoDeleteFile, *args)

    def _io_operation_begin(self, *args):
        self._set_location_event(events.IoOperationBegin, *args)

    def _io_operation_test(self, *args):
        self._set_location_event(events.IoOperationTest, *args)

    def _io_operation_issued(self, *args):
        self._set_location_event(events.IoOperationIssued, *args)

    def _io_operation_complete(self, *args):
        self._set_location_event(events.IoOperationComplete, *args)

    def _io_operation_cancelled(self, *args):
        self._set_location_event(events.IoOperationCancelled, *args)

    def _io_acquire_lock(self, *args):
        self._set_location_event(events.IoAcquireLock, *args)

    def _io_release_lock(self, *args):
        self._set_location_event(events.IoReleaseLock, *args)

    def _io_try_lock(self, *args):
        self._set_location_event(events.IoTryLock, *args)

    def _program_begin(self, *args):
        self._set_location_event(events.ProgramBegin, *args)

    def _program_end(self, *args):
        self._set_location_event(events.ProgramEnd, *args)

    def _non_blocking_collective_request(self, *args):
        self._set_location_event(events.NonBlockingCollectiveRequest, *args)

    def _non_blocking_collective_complete(self, *args):
        self._set_location_event(events.NonBlockingCollectiveComplete, *args)

    def _comm_create(self, *args):
        self._set_location_event(events.CommCreate, *args)

    def _comm_destroy(self, *args):
        self._set_location_event(events.CommDestroy, *args)
