from pathlib import Path
from typing import Callable, List
from unittest.mock import Mock

import pytest
from simon.openfoam.listener import OFListener
from tests.test_openfoam.conftest import (
    create_reconstructed_tars,
    create_reconstructed_timestamps_with_done_marker,
    create_reconstructed_timestamps_without_done_marker,
    create_split_timestamps)


def test_requeues_job_when_split_time_is_found(
    decomposed_case_dir: Path,
    listener: OFListener,
) -> None:
    assert isinstance(listener.cluster, Mock)
    create_split_timestamps(decomposed_case_dir, ["0.1", "0.2"])
    listener.get_new_tasks()
    listener.cluster.requeue_job.assert_called_once()


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_reconstructed_timestamps_with_done_marker,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_tars,
    ],
)
def test_does_not_requeue_job_when_no_split_time_is_found(
    decomposed_case_dir: Path,
    listener: OFListener,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    assert isinstance(listener.cluster, Mock)
    create_extra_files(decomposed_case_dir, ["0.1", "0.2"])
    listener.get_new_tasks()
    listener.cluster.requeue_job.assert_not_called()
