from decimal import Decimal
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from simon.oflistener import OFListener
from tests.test_oflistener._oflistener_test_cases import \
    OFLISTENER_TEST_CASES_SPLIT_ONLY
from tests.test_oflistener.conftest import (
    create_reconstructed_tars,
    create_reconstructed_timestamps_with_done_marker,
    create_reconstructed_timestamps_without_done_marker,
    create_split_timestamps)


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_deletes_split_times_if_not_wanted(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't delete the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "delete":
            required_task = listener._create_delete_split_task(timestamp)
            assert required_task in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_not_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(split_times[-1])
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_partially_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    last_split_time = split_times[-1]
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, [last_split_time]
    )
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(last_split_time)
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_fully_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    last_split_time = split_times[-1]
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, [last_split_time]
    )
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(last_split_time)
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_tarred(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    last_split_time = split_times[-1]
    create_reconstructed_tars(decomposed_case_dir, [last_split_time])
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(last_split_time)
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "num_reconstructed_dirs",
    range(1, len(OFLISTENER_TEST_CASES_SPLIT_ONLY[0][1])),
)
@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_deletes_split_times_if_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    num_reconstructed_dirs: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Reconstruct as many directories as requested by the test
    already_reconstructed_times = split_times[:num_reconstructed_dirs]
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, already_reconstructed_times
    )
    tasks = listener.get_new_tasks()
    for timestamp in already_reconstructed_times:
        required_task = listener._create_delete_split_task(timestamp)
        assert required_task in tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(OFLISTENER_TEST_CASES_SPLIT_ONLY[0][1])),
)
@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_deletes_split_times_if_tarred(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Tar as many directories as requested by the test
    already_tarred_times = split_times[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in already_tarred_times:
        required_task = listener._create_delete_split_task(timestamp)
        assert required_task in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_split_time_if_not_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't delete the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "reconstruct":
            unallowed_task = listener._create_delete_split_task(timestamp)
            assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_split_time_if_partially_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Partially reconstruct every timestamp
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, split_times
    )
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't delete the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "reconstruct":
            unallowed_task = listener._create_delete_split_task(timestamp)
            assert unallowed_task not in tasks
