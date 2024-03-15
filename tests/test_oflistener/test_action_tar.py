from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from simon.oflistener import OFListener
from tests.test_oflistener.conftest import (
    TEST_TIMESTAMP_STRINGS, create_reconstructed_tars,
    create_reconstructed_timestamps_with_done_marker,
    create_reconstructed_timestamps_without_done_marker)


@pytest.mark.parametrize(
    "num_partially_reconstructed_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_does_not_tar_time_if_partially_reconstructed(
    decomposed_case_dir: Path,
    num_partially_reconstructed_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    # Partially reconstruct as many times as requested by the test
    partial_times = TEST_TIMESTAMP_STRINGS[:num_partially_reconstructed_times]
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, partial_times
    )
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        if timestamp in partial_times:
            # If a time is partially reconstructed it should not be deleted
            unallowed_task = listener._create_tar_task(timestamp)
            assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_does_not_tar_time_if_already_tarred(
    decomposed_case_dir: Path,
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, TEST_TIMESTAMP_STRINGS
    )
    # Tar as many directories as requested by the test
    already_tarred_times = TEST_TIMESTAMP_STRINGS[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        if timestamp in already_tarred_times:
            # If a time is tarred, it should not be tarred again
            unallowed_task = listener._create_tar_task(timestamp)
            assert unallowed_task not in tasks


def test_tars_times_if_reconstructed(
    decomposed_case_dir: Path,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, TEST_TIMESTAMP_STRINGS
    )
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        required_task = listener._create_tar_task(timestamp)
        assert required_task in tasks
