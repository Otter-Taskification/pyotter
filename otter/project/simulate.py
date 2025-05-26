from __future__ import annotations

from otter.simulator import simulate_ideal, simulate_finite

from .project import SimulateTrace


def simulate_schedule(anchorfile: str, finite: bool = False, dummy: bool = False, num_threads: int = 1, t_schedule: int = 0, t_pending: int = 0) -> None:
    project = SimulateTrace(anchorfile)
    with project.connect(dummy=dummy) as sim_writer_callbacks:
        project.log_info(f"simulating trace {anchorfile}")
        if finite:
            simulate_finite(project.reader, *sim_writer_callbacks, num_threads=num_threads, t_schedule=t_schedule, t_pending=t_pending)
        else:
            simulate_ideal(project.reader, *sim_writer_callbacks)
