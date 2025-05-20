"""Extensions to otf2"""

from typing import Iterable, List, Tuple, Dict, Any, Type

import _otf2
from otf2 import events
from otf2.attribute_list import AttributeList

from .events import EventType


class GlobalEventReader:
    """Streams events from an OTF2 trace across all locations"""

    def __init__(self, global_evt_reader, definitions, batch_size: int = 100) -> None:
        self._global_evt_reader = global_evt_reader
        self._definitions = definitions
        self._batch_size = batch_size

    def __iter__(self):
        event_buffer: List[Tuple[int, EventType]] = []
        callbacks = _otf2.GlobalEvtReaderCallbacks_New()
        self._set_global_evt_reader_callbacks(callbacks)
        _otf2.GlobalEvtReader_SetCallbacks(self._global_evt_reader, callbacks, event_buffer)
        _otf2.GlobalEvtReaderCallbacks_Delete(callbacks)
        while True:
            event_buffer.clear()
            read_events = _otf2.GlobalEvtReader_ReadEvents(self._global_evt_reader, self._batch_size)
            for location, event in event_buffer:
                yield location, event
            if read_events < self._batch_size:
                break

    def _append(
        self,
        event_type: Type[EventType],
        location_ref: int,
        time,
        event_buffer: List[Tuple[int, EventType]],
        attribute_list,
        *args,
    ):
        event = event_type._construct(self._definitions, time, *args)
        event.attributes = AttributeList._construct(
            self._definitions, attribute_list
        )
        event_buffer.append((location_ref, event))

    def _set_global_evt_reader_callbacks(self, cbs):
        _otf2.GlobalEvtReaderCallbacks_SetBufferFlushCallback(cbs, self._buffer_flush)
        _otf2.GlobalEvtReaderCallbacks_SetMeasurementOnOffCallback(cbs, self._measurement_on_off)
        _otf2.GlobalEvtReaderCallbacks_SetEnterCallback(cbs, self._enter)
        _otf2.GlobalEvtReaderCallbacks_SetLeaveCallback(cbs, self._leave)
        _otf2.GlobalEvtReaderCallbacks_SetMpiSendCallback(cbs, self._mpi_send)
        _otf2.GlobalEvtReaderCallbacks_SetMpiIsendCallback(cbs, self._mpi_isend)
        _otf2.GlobalEvtReaderCallbacks_SetMpiIsendCompleteCallback(cbs, self._mpi_isend_complete)
        _otf2.GlobalEvtReaderCallbacks_SetMpiIrecvRequestCallback(cbs, self._mpi_irecv_request)
        _otf2.GlobalEvtReaderCallbacks_SetMpiRecvCallback(cbs, self._mpi_recv)
        _otf2.GlobalEvtReaderCallbacks_SetMpiIrecvCallback(cbs, self._mpi_irecv)
        _otf2.GlobalEvtReaderCallbacks_SetMpiRequestTestCallback(cbs, self._mpi_request_test)
        _otf2.GlobalEvtReaderCallbacks_SetMpiRequestCancelledCallback(cbs, self._mpi_request_cancelled)
        _otf2.GlobalEvtReaderCallbacks_SetMpiCollectiveBeginCallback(cbs, self._mpi_collective_begin)
        _otf2.GlobalEvtReaderCallbacks_SetMpiCollectiveEndCallback(cbs, self._mpi_collective_end)
        _otf2.GlobalEvtReaderCallbacks_SetOmpForkCallback(cbs, self._omp_fork)
        _otf2.GlobalEvtReaderCallbacks_SetOmpJoinCallback(cbs, self._omp_join)
        _otf2.GlobalEvtReaderCallbacks_SetOmpAcquireLockCallback(cbs, self._omp_acquire_lock)
        _otf2.GlobalEvtReaderCallbacks_SetOmpReleaseLockCallback(cbs, self._omp_release_lock)
        _otf2.GlobalEvtReaderCallbacks_SetOmpTaskCreateCallback(cbs, self._omp_task_create)
        _otf2.GlobalEvtReaderCallbacks_SetOmpTaskSwitchCallback(cbs, self._omp_task_switch)
        _otf2.GlobalEvtReaderCallbacks_SetOmpTaskCompleteCallback(cbs, self._omp_task_complete)
        _otf2.GlobalEvtReaderCallbacks_SetMetricCallback(cbs, self._metric)
        _otf2.GlobalEvtReaderCallbacks_SetParameterStringCallback(cbs, self._parameter_string)
        _otf2.GlobalEvtReaderCallbacks_SetParameterIntCallback(cbs, self._parameter_int)
        _otf2.GlobalEvtReaderCallbacks_SetParameterUnsignedIntCallback(cbs, self._parameter_unsigned_int)
        _otf2.GlobalEvtReaderCallbacks_SetRmaWinCreateCallback(cbs, self._rma_win_create)
        _otf2.GlobalEvtReaderCallbacks_SetRmaWinDestroyCallback(cbs, self._rma_win_destroy)
        _otf2.GlobalEvtReaderCallbacks_SetRmaCollectiveBeginCallback(cbs, self._rma_collective_begin)
        _otf2.GlobalEvtReaderCallbacks_SetRmaCollectiveEndCallback(cbs, self._rma_collective_end)
        _otf2.GlobalEvtReaderCallbacks_SetRmaGroupSyncCallback(cbs, self._rma_group_sync)
        _otf2.GlobalEvtReaderCallbacks_SetRmaRequestLockCallback(cbs, self._rma_request_lock)
        _otf2.GlobalEvtReaderCallbacks_SetRmaAcquireLockCallback(cbs, self._rma_acquire_lock)
        _otf2.GlobalEvtReaderCallbacks_SetRmaTryLockCallback(cbs, self._rma_try_lock)
        _otf2.GlobalEvtReaderCallbacks_SetRmaReleaseLockCallback(cbs, self._rma_release_lock)
        _otf2.GlobalEvtReaderCallbacks_SetRmaSyncCallback(cbs, self._rma_sync)
        _otf2.GlobalEvtReaderCallbacks_SetRmaWaitChangeCallback(cbs, self._rma_wait_change)
        _otf2.GlobalEvtReaderCallbacks_SetRmaPutCallback(cbs, self._rma_put)
        _otf2.GlobalEvtReaderCallbacks_SetRmaGetCallback(cbs, self._rma_get)
        _otf2.GlobalEvtReaderCallbacks_SetRmaAtomicCallback(cbs, self._rma_atomic)
        _otf2.GlobalEvtReaderCallbacks_SetRmaOpCompleteBlockingCallback(cbs, self._rma_op_complete_blocking)
        _otf2.GlobalEvtReaderCallbacks_SetRmaOpCompleteNonBlockingCallback(cbs, self._rma_op_complete_non_blocking)
        _otf2.GlobalEvtReaderCallbacks_SetRmaOpTestCallback(cbs, self._rma_op_test)
        _otf2.GlobalEvtReaderCallbacks_SetRmaOpCompleteRemoteCallback(cbs, self._rma_op_complete_remote)
        _otf2.GlobalEvtReaderCallbacks_SetThreadForkCallback(cbs, self._thread_fork)
        _otf2.GlobalEvtReaderCallbacks_SetThreadJoinCallback(cbs, self._thread_join)
        _otf2.GlobalEvtReaderCallbacks_SetThreadTeamBeginCallback(cbs, self._thread_team_begin)
        _otf2.GlobalEvtReaderCallbacks_SetThreadTeamEndCallback(cbs, self._thread_team_end)
        _otf2.GlobalEvtReaderCallbacks_SetThreadAcquireLockCallback(cbs, self._thread_acquire_lock)
        _otf2.GlobalEvtReaderCallbacks_SetThreadReleaseLockCallback(cbs, self._thread_release_lock)
        _otf2.GlobalEvtReaderCallbacks_SetThreadTaskCreateCallback(cbs, self._thread_task_create)
        _otf2.GlobalEvtReaderCallbacks_SetThreadTaskSwitchCallback(cbs, self._thread_task_switch)
        _otf2.GlobalEvtReaderCallbacks_SetThreadTaskCompleteCallback(cbs, self._thread_task_complete)
        _otf2.GlobalEvtReaderCallbacks_SetThreadCreateCallback(cbs, self._thread_create)
        _otf2.GlobalEvtReaderCallbacks_SetThreadBeginCallback(cbs, self._thread_begin)
        _otf2.GlobalEvtReaderCallbacks_SetThreadWaitCallback(cbs, self._thread_wait)
        _otf2.GlobalEvtReaderCallbacks_SetThreadEndCallback(cbs, self._thread_end)
        _otf2.GlobalEvtReaderCallbacks_SetCallingContextEnterCallback(cbs, self._calling_context_enter)
        _otf2.GlobalEvtReaderCallbacks_SetCallingContextLeaveCallback(cbs, self._calling_context_leave)
        _otf2.GlobalEvtReaderCallbacks_SetCallingContextSampleCallback(cbs, self._calling_context_sample)
        _otf2.GlobalEvtReaderCallbacks_SetIoCreateHandleCallback(cbs, self._io_create_handle)
        _otf2.GlobalEvtReaderCallbacks_SetIoDestroyHandleCallback(cbs, self._io_destroy_handle)
        _otf2.GlobalEvtReaderCallbacks_SetIoDuplicateHandleCallback(cbs, self._io_duplicate_handle)
        _otf2.GlobalEvtReaderCallbacks_SetIoSeekCallback(cbs, self._io_seek)
        _otf2.GlobalEvtReaderCallbacks_SetIoChangeStatusFlagsCallback(cbs, self._io_change_status_flags)
        _otf2.GlobalEvtReaderCallbacks_SetIoDeleteFileCallback(cbs, self._io_delete_file)
        _otf2.GlobalEvtReaderCallbacks_SetIoOperationBeginCallback(cbs, self._io_operation_begin)
        _otf2.GlobalEvtReaderCallbacks_SetIoOperationTestCallback(cbs, self._io_operation_test)
        _otf2.GlobalEvtReaderCallbacks_SetIoOperationIssuedCallback(cbs, self._io_operation_issued)
        _otf2.GlobalEvtReaderCallbacks_SetIoOperationCompleteCallback(cbs, self._io_operation_complete)
        _otf2.GlobalEvtReaderCallbacks_SetIoOperationCancelledCallback(cbs, self._io_operation_cancelled)
        _otf2.GlobalEvtReaderCallbacks_SetIoAcquireLockCallback(cbs, self._io_acquire_lock)
        _otf2.GlobalEvtReaderCallbacks_SetIoReleaseLockCallback(cbs, self._io_release_lock)
        _otf2.GlobalEvtReaderCallbacks_SetIoTryLockCallback(cbs, self._io_try_lock)
        _otf2.GlobalEvtReaderCallbacks_SetProgramBeginCallback(cbs, self._program_begin)
        _otf2.GlobalEvtReaderCallbacks_SetProgramEndCallback(cbs, self._program_end)
        _otf2.GlobalEvtReaderCallbacks_SetNonBlockingCollectiveRequestCallback(cbs, self._non_blocking_collective_request)
        _otf2.GlobalEvtReaderCallbacks_SetNonBlockingCollectiveCompleteCallback(cbs, self._non_blocking_collective_complete)
        _otf2.GlobalEvtReaderCallbacks_SetCommCreateCallback(cbs, self._comm_create)
        _otf2.GlobalEvtReaderCallbacks_SetCommDestroyCallback(cbs, self._comm_destroy)

    def _buffer_flush(self, *args):
        self._append(events.BufferFlush, *args)

    def _measurement_on_off(self, *args):
        self._append(events.MeasurementOnOff, *args)

    def _enter(self, *args):
        self._append(events.Enter, *args)

    def _leave(self, *args):
        self._append(events.Leave, *args)

    def _mpi_send(self, *args):
        self._append(events.MpiSend, *args)

    def _mpi_isend(self, *args):
        self._append(events.MpiIsend, *args)

    def _mpi_isend_complete(self, *args):
        self._append(events.MpiIsendComplete, *args)

    def _mpi_irecv_request(self, *args):
        self._append(events.MpiIrecvRequest, *args)

    def _mpi_recv(self, *args):
        self._append(events.MpiRecv, *args)

    def _mpi_irecv(self, *args):
        self._append(events.MpiIrecv, *args)

    def _mpi_request_test(self, *args):
        self._append(events.MpiRequestTest, *args)

    def _mpi_request_cancelled(self, *args):
        self._append(events.MpiRequestCancelled, *args)

    def _mpi_collective_begin(self, *args):
        self._append(events.MpiCollectiveBegin, *args)

    def _mpi_collective_end(self, *args):
        self._append(events.MpiCollectiveEnd, *args)

    def _omp_fork(self, *args):
        self._append(events.OmpFork, *args)

    def _omp_join(self, *args):
        self._append(events.OmpJoin, *args)

    def _omp_acquire_lock(self, *args):
        self._append(events.OmpAcquireLock, *args)

    def _omp_release_lock(self, *args):
        self._append(events.OmpReleaseLock, *args)

    def _omp_task_create(self, *args):
        self._append(events.OmpTaskCreate, *args)

    def _omp_task_switch(self, *args):
        self._append(events.OmpTaskSwitch, *args)

    def _omp_task_complete(self, *args):
        self._append(events.OmpTaskComplete, *args)

    def _metric(self, *args):
        self._append(events.Metric, *args)

    def _parameter_string(self, *args):
        self._append(events.ParameterString, *args)

    def _parameter_int(self, *args):
        self._append(events.ParameterInt, *args)

    def _parameter_unsigned_int(self, *args):
        self._append(events.ParameterUnsignedInt, *args)

    def _rma_win_create(self, *args):
        self._append(events.RmaWinCreate, *args)

    def _rma_win_destroy(self, *args):
        self._append(events.RmaWinDestroy, *args)

    def _rma_collective_begin(self, *args):
        self._append(events.RmaCollectiveBegin, *args)

    def _rma_collective_end(self, *args):
        self._append(events.RmaCollectiveEnd, *args)

    def _rma_group_sync(self, *args):
        self._append(events.RmaGroupSync, *args)

    def _rma_request_lock(self, *args):
        self._append(events.RmaRequestLock, *args)

    def _rma_acquire_lock(self, *args):
        self._append(events.RmaAcquireLock, *args)

    def _rma_try_lock(self, *args):
        self._append(events.RmaTryLock, *args)

    def _rma_release_lock(self, *args):
        self._append(events.RmaReleaseLock, *args)

    def _rma_sync(self, *args):
        self._append(events.RmaSync, *args)

    def _rma_wait_change(self, *args):
        self._append(events.RmaWaitChange, *args)

    def _rma_put(self, *args):
        self._append(events.RmaPut, *args)

    def _rma_get(self, *args):
        self._append(events.RmaGet, *args)

    def _rma_atomic(self, *args):
        self._append(events.RmaAtomic, *args)

    def _rma_op_complete_blocking(self, *args):
        self._append(events.RmaOpCompleteBlocking, *args)

    def _rma_op_complete_non_blocking(self, *args):
        self._append(events.RmaOpCompleteNonBlocking, *args)

    def _rma_op_test(self, *args):
        self._append(events.RmaOpTest, *args)

    def _rma_op_complete_remote(self, *args):
        self._append(events.RmaOpCompleteRemote, *args)

    def _thread_fork(self, *args):
        self._append(events.ThreadFork, *args)

    def _thread_join(self, *args):
        self._append(events.ThreadJoin, *args)

    def _thread_team_begin(self, *args):
        self._append(events.ThreadTeamBegin, *args)

    def _thread_team_end(self, *args):
        self._append(events.ThreadTeamEnd, *args)

    def _thread_acquire_lock(self, *args):
        self._append(events.ThreadAcquireLock, *args)

    def _thread_release_lock(self, *args):
        self._append(events.ThreadReleaseLock, *args)

    def _thread_task_create(self, *args):
        self._append(events.ThreadTaskCreate, *args)

    def _thread_task_switch(self, *args):
        self._append(events.ThreadTaskSwitch, *args)

    def _thread_task_complete(self, *args):
        self._append(events.ThreadTaskComplete, *args)

    def _thread_create(self, *args):
        self._append(events.ThreadCreate, *args)

    def _thread_begin(self, *args):
        self._append(events.ThreadBegin, *args)

    def _thread_wait(self, *args):
        self._append(events.ThreadWait, *args)

    def _thread_end(self, *args):
        self._append(events.ThreadEnd, *args)

    def _calling_context_enter(self, *args):
        self._append(events.CallingContextEnter, *args)

    def _calling_context_leave(self, *args):
        self._append(events.CallingContextLeave, *args)

    def _calling_context_sample(self, *args):
        self._append(events.CallingContextSample, *args)

    def _io_create_handle(self, *args):
        self._append(events.IoCreateHandle, *args)

    def _io_destroy_handle(self, *args):
        self._append(events.IoDestroyHandle, *args)

    def _io_duplicate_handle(self, *args):
        self._append(events.IoDuplicateHandle, *args)

    def _io_seek(self, *args):
        self._append(events.IoSeek, *args)

    def _io_change_status_flags(self, *args):
        self._append(events.IoChangeStatusFlags, *args)

    def _io_delete_file(self, *args):
        self._append(events.IoDeleteFile, *args)

    def _io_operation_begin(self, *args):
        self._append(events.IoOperationBegin, *args)

    def _io_operation_test(self, *args):
        self._append(events.IoOperationTest, *args)

    def _io_operation_issued(self, *args):
        self._append(events.IoOperationIssued, *args)

    def _io_operation_complete(self, *args):
        self._append(events.IoOperationComplete, *args)

    def _io_operation_cancelled(self, *args):
        self._append(events.IoOperationCancelled, *args)

    def _io_acquire_lock(self, *args):
        self._append(events.IoAcquireLock, *args)

    def _io_release_lock(self, *args):
        self._append(events.IoReleaseLock, *args)

    def _io_try_lock(self, *args):
        self._append(events.IoTryLock, *args)

    def _program_begin(self, *args):
        self._append(events.ProgramBegin, *args)

    def _program_end(self, *args):
        self._append(events.ProgramEnd, *args)

    def _non_blocking_collective_request(self, *args):
        self._append(events.NonBlockingCollectiveRequest, *args)

    def _non_blocking_collective_complete(self, *args):
        self._append(events.NonBlockingCollectiveComplete, *args)

    def _comm_create(self, *args):
        self._append(events.CommCreate, *args)

    def _comm_destroy(self, *args):
        self._append(events.CommDestroy, *args)


class SeekingEventReader:
    """Read events from an OTF2 trace at selected positions & locations"""

    def __init__(self, evt_readers: Dict[int, Any], definitions) -> None:
        self._evt_readers = evt_readers
        self._definitions = definitions

    def events(self, positions: Iterable[Tuple[int, int]], batch_size: int = 100):
        event_buffer: List[Tuple[int, Tuple[int, EventType]]] = []
        callbacks = _otf2.EvtReaderCallbacks_New()
        self._set_event_reader_callbacks(callbacks)
        for evt_reader in self._evt_readers.values():
            _otf2.EvtReader_SetCallbacks(evt_reader, callbacks, event_buffer)
        _otf2.EvtReaderCallbacks_Delete(callbacks)
        batch_read = 0
        for location_ref, pos in positions:
            evt_reader = self._evt_readers[location_ref]
            _otf2.EvtReader_Seek(evt_reader, pos)
            events_read = _otf2.EvtReader_ReadEvents(evt_reader, 1)
            if events_read == 0:
                raise ValueError(f"failed to read event at {location_ref=}, {pos=}")
            batch_read += events_read
            if batch_read >= batch_size:
                yield from self._yield_events(event_buffer)
                event_buffer.clear()

        # yield any items remaining in the buffer if we read part of a batch
        yield from self._yield_events(event_buffer)

    def _yield_events(self, event_buffer: Iterable[Tuple[int, Tuple[int, EventType]]]):
        for event_position, (location, event) in event_buffer:
            yield event_position, (location, event)

    def _append(
        self,
        event_type: Type[EventType],
        location_ref: int,
        time,
        event_position: int,
        event_buffer: List[Tuple[int, Tuple[int, EventType]]],
        attribute_list,
        *args
    ):
        event = event_type._construct(self._definitions, time, *args)
        event.attributes = AttributeList._construct(
            self._definitions, attribute_list
        )
        item = event_position, (location_ref, event)
        event_buffer.append(item)

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
        _otf2.EvtReaderCallbacks_SetNonBlockingCollectiveRequestCallback(cbs, self._non_blocking_collective_request)
        _otf2.EvtReaderCallbacks_SetNonBlockingCollectiveCompleteCallback(cbs, self._non_blocking_collective_complete)
        _otf2.EvtReaderCallbacks_SetCommCreateCallback(cbs, self._comm_create)
        _otf2.EvtReaderCallbacks_SetCommDestroyCallback(cbs, self._comm_destroy)

    def _buffer_flush(self, *args):
        self._append(events.BufferFlush, *args)

    def _measurement_on_off(self, *args):
        self._append(events.MeasurementOnOff, *args)

    def _enter(self, *args):
        self._append(events.Enter, *args)

    def _leave(self, *args):
        self._append(events.Leave, *args)

    def _mpi_send(self, *args):
        self._append(events.MpiSend, *args)

    def _mpi_isend(self, *args):
        self._append(events.MpiIsend, *args)

    def _mpi_isend_complete(self, *args):
        self._append(events.MpiIsendComplete, *args)

    def _mpi_irecv_request(self, *args):
        self._append(events.MpiIrecvRequest, *args)

    def _mpi_recv(self, *args):
        self._append(events.MpiRecv, *args)

    def _mpi_irecv(self, *args):
        self._append(events.MpiIrecv, *args)

    def _mpi_request_test(self, *args):
        self._append(events.MpiRequestTest, *args)

    def _mpi_request_cancelled(self, *args):
        self._append(events.MpiRequestCancelled, *args)

    def _mpi_collective_begin(self, *args):
        self._append(events.MpiCollectiveBegin, *args)

    def _mpi_collective_end(self, *args):
        self._append(events.MpiCollectiveEnd, *args)

    def _omp_fork(self, *args):
        self._append(events.OmpFork, *args)

    def _omp_join(self, *args):
        self._append(events.OmpJoin, *args)

    def _omp_acquire_lock(self, *args):
        self._append(events.OmpAcquireLock, *args)

    def _omp_release_lock(self, *args):
        self._append(events.OmpReleaseLock, *args)

    def _omp_task_create(self, *args):
        self._append(events.OmpTaskCreate, *args)

    def _omp_task_switch(self, *args):
        self._append(events.OmpTaskSwitch, *args)

    def _omp_task_complete(self, *args):
        self._append(events.OmpTaskComplete, *args)

    def _metric(self, *args):
        self._append(events.Metric, *args)

    def _parameter_string(self, *args):
        self._append(events.ParameterString, *args)

    def _parameter_int(self, *args):
        self._append(events.ParameterInt, *args)

    def _parameter_unsigned_int(self, *args):
        self._append(events.ParameterUnsignedInt, *args)

    def _rma_win_create(self, *args):
        self._append(events.RmaWinCreate, *args)

    def _rma_win_destroy(self, *args):
        self._append(events.RmaWinDestroy, *args)

    def _rma_collective_begin(self, *args):
        self._append(events.RmaCollectiveBegin, *args)

    def _rma_collective_end(self, *args):
        self._append(events.RmaCollectiveEnd, *args)

    def _rma_group_sync(self, *args):
        self._append(events.RmaGroupSync, *args)

    def _rma_request_lock(self, *args):
        self._append(events.RmaRequestLock, *args)

    def _rma_acquire_lock(self, *args):
        self._append(events.RmaAcquireLock, *args)

    def _rma_try_lock(self, *args):
        self._append(events.RmaTryLock, *args)

    def _rma_release_lock(self, *args):
        self._append(events.RmaReleaseLock, *args)

    def _rma_sync(self, *args):
        self._append(events.RmaSync, *args)

    def _rma_wait_change(self, *args):
        self._append(events.RmaWaitChange, *args)

    def _rma_put(self, *args):
        self._append(events.RmaPut, *args)

    def _rma_get(self, *args):
        self._append(events.RmaGet, *args)

    def _rma_atomic(self, *args):
        self._append(events.RmaAtomic, *args)

    def _rma_op_complete_blocking(self, *args):
        self._append(events.RmaOpCompleteBlocking, *args)

    def _rma_op_complete_non_blocking(self, *args):
        self._append(events.RmaOpCompleteNonBlocking, *args)

    def _rma_op_test(self, *args):
        self._append(events.RmaOpTest, *args)

    def _rma_op_complete_remote(self, *args):
        self._append(events.RmaOpCompleteRemote, *args)

    def _thread_fork(self, *args):
        self._append(events.ThreadFork, *args)

    def _thread_join(self, *args):
        self._append(events.ThreadJoin, *args)

    def _thread_team_begin(self, *args):
        self._append(events.ThreadTeamBegin, *args)

    def _thread_team_end(self, *args):
        self._append(events.ThreadTeamEnd, *args)

    def _thread_acquire_lock(self, *args):
        self._append(events.ThreadAcquireLock, *args)

    def _thread_release_lock(self, *args):
        self._append(events.ThreadReleaseLock, *args)

    def _thread_task_create(self, *args):
        self._append(events.ThreadTaskCreate, *args)

    def _thread_task_switch(self, *args):
        self._append(events.ThreadTaskSwitch, *args)

    def _thread_task_complete(self, *args):
        self._append(events.ThreadTaskComplete, *args)

    def _thread_create(self, *args):
        self._append(events.ThreadCreate, *args)

    def _thread_begin(self, *args):
        self._append(events.ThreadBegin, *args)

    def _thread_wait(self, *args):
        self._append(events.ThreadWait, *args)

    def _thread_end(self, *args):
        self._append(events.ThreadEnd, *args)

    def _calling_context_enter(self, *args):
        self._append(events.CallingContextEnter, *args)

    def _calling_context_leave(self, *args):
        self._append(events.CallingContextLeave, *args)

    def _calling_context_sample(self, *args):
        self._append(events.CallingContextSample, *args)

    def _io_create_handle(self, *args):
        self._append(events.IoCreateHandle, *args)

    def _io_destroy_handle(self, *args):
        self._append(events.IoDestroyHandle, *args)

    def _io_duplicate_handle(self, *args):
        self._append(events.IoDuplicateHandle, *args)

    def _io_seek(self, *args):
        self._append(events.IoSeek, *args)

    def _io_change_status_flags(self, *args):
        self._append(events.IoChangeStatusFlags, *args)

    def _io_delete_file(self, *args):
        self._append(events.IoDeleteFile, *args)

    def _io_operation_begin(self, *args):
        self._append(events.IoOperationBegin, *args)

    def _io_operation_test(self, *args):
        self._append(events.IoOperationTest, *args)

    def _io_operation_issued(self, *args):
        self._append(events.IoOperationIssued, *args)

    def _io_operation_complete(self, *args):
        self._append(events.IoOperationComplete, *args)

    def _io_operation_cancelled(self, *args):
        self._append(events.IoOperationCancelled, *args)

    def _io_acquire_lock(self, *args):
        self._append(events.IoAcquireLock, *args)

    def _io_release_lock(self, *args):
        self._append(events.IoReleaseLock, *args)

    def _io_try_lock(self, *args):
        self._append(events.IoTryLock, *args)

    def _program_begin(self, *args):
        self._append(events.ProgramBegin, *args)

    def _program_end(self, *args):
        self._append(events.ProgramEnd, *args)

    def _non_blocking_collective_request(self, *args):
        self._append(events.NonBlockingCollectiveRequest, *args)

    def _non_blocking_collective_complete(self, *args):
        self._append(events.NonBlockingCollectiveComplete, *args)

    def _comm_create(self, *args):
        self._append(events.CommCreate, *args)

    def _comm_destroy(self, *args):
        self._append(events.CommDestroy, *args)
