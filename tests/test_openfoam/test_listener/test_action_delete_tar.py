from decimal import Decimal
from typing import List

import pytest
from simon.openfoam.listener import OFListener
from tests.test_openfoam.conftest import (create_compressed_files,
                                          create_reconstructed_tars)


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.1", "0.15"],  # Missing 0.05
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.1", "0.15"],  # Missing 0.05
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.95", "1", "1.05"],  # Missing 0.9
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.85", "0.9", "0.95", "1", "1.05"],  # Missing 0.8
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.15", "1.2"],  # Missing 1.1
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.05",
                "0.1",
                "0.2",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.15
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.2",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.3
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.1",
                "0.15",
                "0.2",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.05 and 0.3
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.2
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.15",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.2 and 0.1
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.2 and 0.3
        ),
    ],
)
def test_does_not_delete_tarred_times_if_nothing_compressed(
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    listener: OFListener,
) -> None:
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(listener.state.case_dir, tars_present)
    tasks = listener.get_new_tasks()
    for t in tars_present:
        forbidden_task = listener._create_delete_tar_task(t)
        assert forbidden_task not in tasks


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, tgzs_present, tars_to_preserve",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15"],
            ["times_0.2_0.35_0.05.tgz"],
            ["0", "0.05", "0.1", "0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15"],
            ["times_0_0.1_0.05.tgz"],
            ["0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            ["times_0.8_0.95_0.05.tgz"],
            ["1", "1.05"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            ["times_0.8_0.95_0.05.tgz"],
            ["0.75", "1", "1.05"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["times_1_1.15_0.05.tgz"],
            ["0.9", "0.95", "1.2"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0_0.15_0.05.tgz"],
            ["0.2", "0.25", "0.3", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0.15_0.25_0.05.tgz"],
            ["0", "0.05", "0.1", "0.3", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_1_1.15_0.05.tgz"],
            ["0.8", "0.85", "0.9", "0.95"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["times_0.8_0.95_0.05.tgz"],
            ["1", "1.05", "1.1", "1.15", "1.2"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
            ["0.75"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.75_0.85_0.05.tgz", "times_1.05_1.15_0.05.tgz"],
            ["0.9", "0.95", "1"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.05",
                "0.1",
                "0.2",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.15
            ["times_0.2_0.35_0.05.tgz"],
            ["0", "0.05", "0.1", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.2",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.3
            ["times_0_0.15_0.05.tgz"],
            ["0.2", "0.25", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.2
            ["times_0_0.1_0.05.tgz", "times_0.3_0.4_0.05.tgz"],
            ["0.15", "0.25"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.15",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.2 and 0.1
            ["times_0.3_0.4_0.05.tgz"],
            ["0", "0.05", "0.15", "0.25"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.2 and 0.3
            ["times_0_0.1_0.05.tgz"],
            ["0.15", "0.25", "0.35", "0.4"],
        ),
    ],
)
def test_does_not_delete_tarred_times_when_other_tgzs_are_present(
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    tgzs_present: List[str],
    tars_to_preserve: List[str],
    listener: OFListener,
) -> None:
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(listener.state.case_dir, tars_present)
    create_compressed_files(listener.state.case_dir, tgzs_present)
    tasks = listener.get_new_tasks()
    for t in tars_to_preserve:
        forbidden_task = listener._create_delete_tar_task(t)
        assert forbidden_task not in tasks


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, tgzs_present, tars_to_delete",
    [
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15"],
            ["times_0_0.1_0.05.tgz"],
            ["0", "0.05", "0.1"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            ["times_0.8_0.95_0.05.tgz"],
            ["0.8", "0.85", "0.9", "0.95"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            ["times_0.8_0.95_0.05.tgz"],
            ["0.8", "0.85", "0.9", "0.95"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["times_1_1.15_0.05.tgz"],
            ["1", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0_0.15_0.05.tgz"],
            ["0", "0.05", "0.1", "0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0.15_0.25_0.05.tgz"],
            ["0.15", "0.2", "0.25"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_1_1.15_0.05.tgz"],
            ["1", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["times_0.8_0.95_0.05.tgz"],
            ["0.8", "0.85", "0.9", "0.95"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.75_0.85_0.05.tgz", "times_1.05_1.15_0.05.tgz"],
            ["0.75", "0.8", "0.85", "1.05", "1.1", "1.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.05",
                "0.1",
                "0.2",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.15
            ["times_0.2_0.35_0.05.tgz"],
            ["0.2", "0.25", "0.3", "0.35"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.2",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.3
            ["times_0_0.15_0.05.tgz"],
            ["0", "0.05", "0.1", "0.15"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.2
            ["times_0_0.1_0.05.tgz", "times_0.3_0.4_0.05.tgz"],
            ["0", "0.05", "0.1", "0.3", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.15",
                "0.25",
                "0.3",
                "0.35",
                "0.4",
            ],  # Missing 0.2 and 0.1
            ["times_0.3_0.4_0.05.tgz"],
            ["0.3", "0.35", "0.4"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            [
                "0",
                "0.05",
                "0.1",
                "0.15",
                "0.25",
                "0.35",
                "0.4",
            ],  # Missing 0.2 and 0.3
            ["times_0_0.1_0.05.tgz"],
            ["0", "0.05", "0.1"],
        ),
    ],
)
def test_deletes_tarred_times_when_compressed(
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    tgzs_present: List[str],
    tars_to_delete: List[str],
    listener: OFListener,
) -> None:
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(listener.state.case_dir, tars_present)
    create_compressed_files(listener.state.case_dir, tgzs_present)
    tasks = listener.get_new_tasks()
    for t in tars_to_delete:
        required_task = listener._create_delete_tar_task(t)
        print(required_task)
        print(tasks)
        print(required_task in tasks)
        assert required_task in tasks


def test_does_not_create_duplicate_tar_deletion_tasks(
    listener: OFListener,
) -> None:
    create_compressed_files(listener.state.case_dir, ["times_0_0.15_0.05.tgz"])
    times = ["0", "0.05", "0.1", "0.15", "0.2"]
    times_to_delete = ["0", "0.05", "0.1", "0.15"]
    create_reconstructed_tars(listener.state.case_dir, times)
    tasks = listener.get_new_tasks()
    # Make sure that the tar deletion tasks did get created
    for t in times_to_delete:
        required_task = listener._create_delete_tar_task(t)
        assert required_task in tasks
    # Running again should not create any new tar deletion tasks
    tasks = listener.get_new_tasks()
    assert len(tasks) == 0
