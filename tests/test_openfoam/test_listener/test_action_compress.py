from decimal import Decimal
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from simon.openfoam.listener import OFListener
from tests.test_openfoam.conftest import (create_compressed_files,
                                          create_reconstructed_tars)


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, tars_compressed, tgz_filename",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15"],
            ["0", "0.05", "0.1", "0.15"],
            "times_0_0.15_0.05.tgz",
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15"],
            ["0", "0.05", "0.1"],
            "times_0_0.1_0.05.tgz",
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            ["0.8", "0.85", "0.9", "0.95"],
            "times_0.8_0.95_0.05.tgz",
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            ["0.8", "0.85", "0.9", "0.95"],
            "times_0.8_0.95_0.05.tgz",
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["1", "1.05", "1.1", "1.15"],
            "times_1_1.15_0.05.tgz",
        ),
    ],
)
def test_compresses_when_single_compression_candidate_exists(
    decomposed_case_dir: Path,
    listener: OFListener,
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    tars_compressed: List[str],
    tgz_filename: str,
) -> None:
    assert isinstance(listener.cluster, Mock)
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(decomposed_case_dir, tars_present)
    listener.get_new_tasks()
    listener.cluster.compress.assert_called_once()
    listener.cluster.compress.assert_called_with(tgz_filename, tars_compressed)


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, tars_compressed_list, tgz_filenames",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            [["0", "0.05", "0.1", "0.15"], ["0.2", "0.25", "0.3", "0.35"]],
            ["times_0_0.15_0.05.tgz", "times_0.2_0.35_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            [
                ["0", "0.05", "0.1"],
                ["0.15", "0.2", "0.25"],
                ["0.3", "0.35", "0.4"],
            ],
            [
                "times_0_0.1_0.05.tgz",
                "times_0.15_0.25_0.05.tgz",
                "times_0.3_0.4_0.05.tgz",
            ],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            [["0.8", "0.85", "0.9", "0.95"], ["1", "1.05", "1.1", "1.15"]],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            [["0.8", "0.85", "0.9", "0.95"], ["1", "1.05", "1.1", "1.15"]],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            [["0.8", "0.85", "0.9", "0.95"], ["1", "1.05", "1.1", "1.15"]],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            [
                ["0.75", "0.8", "0.85"],
                ["0.9", "0.95", "1"],
                ["1.05", "1.1", "1.15"],
            ],
            [
                "times_0.75_0.85_0.05.tgz",
                "times_0.9_1_0.05.tgz",
                "times_1.05_1.15_0.05.tgz",
            ],
        ),
    ],
)
def test_compresses_when_multiple_compression_candidates_exist(
    decomposed_case_dir: Path,
    listener: OFListener,
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    tars_compressed_list: List[List[str]],
    tgz_filenames: List[str],
) -> None:
    assert isinstance(listener.cluster, Mock)
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(decomposed_case_dir, tars_present)
    listener.get_new_tasks()
    listener.cluster.compress.assert_called()
    assert listener.cluster.compress.call_count == len(tgz_filenames)
    for compress_call, tgz_filename, tars_compressed in zip(
        listener.cluster.compress.mock_calls,
        tgz_filenames,
        tars_compressed_list,
    ):
        compress_call.args == (tgz_filename, tars_compressed)


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, compressed_files_present, tars_compressed_list, tgz_filenames",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0_0.15_0.05.tgz"],
            [["0.2", "0.25", "0.3", "0.35"]],
            ["times_0.2_0.35_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0.15_0.25_0.05.tgz"],
            [
                ["0", "0.05", "0.1"],
                ["0.3", "0.35", "0.4"],
            ],
            [
                "times_0_0.1_0.05.tgz",
                "times_0.3_0.4_0.05.tgz",
            ],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_1_1.15_0.05.tgz"],
            [["0.8", "0.85", "0.9", "0.95"]],
            ["times_0.8_0.95_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["times_0.8_0.95_0.05.tgz"],
            [["1", "1.05", "1.1", "1.15"]],
            ["times_1_1.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
            [],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.75_0.85_0.05.tgz", "times_1.05_1.15_0.05.tgz"],
            [["0.9", "0.95", "1"]],
            ["times_0.9_1_0.05.tgz"],
        ),
    ],
)
def test_does_not_compress_if_already_compressed(
    decomposed_case_dir: Path,
    listener: OFListener,
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    compressed_files_present: List[str],
    tars_compressed_list: List[List[str]],
    tgz_filenames: List[str],
) -> None:
    assert isinstance(listener.cluster, Mock)
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(decomposed_case_dir, tars_present)
    create_compressed_files(decomposed_case_dir, compressed_files_present)
    listener.get_new_tasks()
    assert listener.cluster.compress.call_count == len(tgz_filenames)
    for compress_call, tgz_filename, tars_compressed in zip(
        listener.cluster.compress.mock_calls,
        tgz_filenames,
        tars_compressed_list,
    ):
        compress_call.args == (tgz_filename, tars_compressed)


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, compressed_files_present",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0_0.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.05", "0.1", "0.15", "0.2", "0.25", "0.3", "0.35", "0.4"],
            ["times_0.15_0.25_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_1_1.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15", "1.2"],
            ["times_0.8_0.95_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.8_0.95_0.05.tgz", "times_1_1.15_0.05.tgz"],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0.75", "0.8", "0.85", "0.9", "0.95", "1", "1.05", "1.1", "1.15"],
            ["times_0.75_0.85_0.05.tgz", "times_1.05_1.15_0.05.tgz"],
        ),
    ],
)
def test_does_not_compress_if_already_requested(
    decomposed_case_dir: Path,
    listener: OFListener,
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    compressed_files_present: List[str],
) -> None:
    assert isinstance(listener.cluster, Mock)
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(decomposed_case_dir, tars_present)
    create_compressed_files(decomposed_case_dir, compressed_files_present)
    listener.get_new_tasks()
    # We don't care about these tasks that are generated the first time on
    # these set of files so reset the mock
    listener.cluster.reset_mock()
    listener.get_new_tasks()
    listener.cluster.compress.assert_not_called()


@pytest.mark.parametrize(
    "keep_every, compress_every, tars_present, tars_compressed_list, tgz_filenames",
    [
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0", "0.1", "0.15"],  # Missing 0.05
            [],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.15"),
            ["0", "0.1", "0.15"],  # Missing 0.05
            [],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.8", "0.85", "0.95", "1", "1.05"],  # Missing 0.9
            [],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.75", "0.85", "0.9", "0.95", "1", "1.05"],  # Missing 0.8
            [],
            [],
        ),
        (
            Decimal("0.05"),
            Decimal("0.2"),
            ["0.9", "0.95", "1", "1.05", "1.15", "1.2"],  # Missing 1.1
            [],
            [],
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
            [["0.2", "0.25", "0.3", "0.35"]],
            ["times_0.2_0.35_0.05.tgz"],
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
            [["0", "0.05", "0.1", "0.15"]],
            ["times_0_0.15_0.05.tgz"],
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
            [],
            [],
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
            [
                ["0", "0.05", "0.1"],
                ["0.3", "0.35", "0.4"],
            ],
            [
                "times_0_0.1_0.05.tgz",
                "times_0.3_0.4_0.05.tgz",
            ],
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
            [["0.3", "0.35", "0.4"]],
            ["times_0.3_0.4_0.05.tgz"],
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
            [["0", "0.05", "0.1"]],
            ["times_0_0.1_0.05.tgz"],
        ),
    ],
)
def test_does_not_compress_when_tars_in_sequence_are_missing(
    decomposed_case_dir: Path,
    listener: OFListener,
    keep_every: Decimal,
    compress_every: Decimal,
    tars_present: List[str],
    tars_compressed_list: List[List[str]],
    tgz_filenames: List[str],
) -> None:
    assert isinstance(listener.cluster, Mock)
    listener.update_processing_frequencies(
        keep_every=keep_every, compress_every=compress_every
    )
    create_reconstructed_tars(decomposed_case_dir, tars_present)
    listener.get_new_tasks()
    true_call_count = len(tgz_filenames)
    assert listener.cluster.compress.call_count == true_call_count
    if true_call_count == 0:
        return
    for compress_call, tgz_filename, tars_compressed in zip(
        listener.cluster.compress.mock_calls,
        tgz_filenames,
        tars_compressed_list,
    ):
        compress_call.args == (tgz_filename, tars_compressed)
