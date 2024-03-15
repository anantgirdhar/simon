from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from simon.oflistener import RECONSTRUCTION_DONE_MARKER_FILENAME, OFListener
from tests.test_oflistener.conftest import TEST_TIMESTAMP_STRINGS


@pytest.mark.parametrize("timestamp", TEST_TIMESTAMP_STRINGS)
def test_reconstruct_task_command(
    decomposed_case_dir: Path, timestamp: str, cluster: Mock
) -> None:
    listener = OFListener(
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    reconstruction_done_marker_filepath = (
        decomposed_case_dir / timestamp / RECONSTRUCTION_DONE_MARKER_FILENAME
    )
    true_command = (
        f"reconstructPar -time {timestamp} -case {decomposed_case_dir}"
        f" && touch {reconstruction_done_marker_filepath}"
    )
    generated_command = listener._create_reconstruct_task(timestamp).command
    assert generated_command == true_command


@pytest.mark.parametrize("timestamp", TEST_TIMESTAMP_STRINGS)
def test_delete_split_task_command(
    decomposed_case_dir: Path, timestamp: str, cluster: Mock
) -> None:
    listener = OFListener(
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    true_command = f"rm -rf {decomposed_case_dir}/processor*/{timestamp}"
    generated_command = listener._create_delete_split_task(timestamp).command
    assert generated_command == true_command


@pytest.mark.parametrize("timestamp", TEST_TIMESTAMP_STRINGS)
def test_delete_reconstructed_task_command(
    decomposed_case_dir: Path, timestamp: str, cluster: Mock
) -> None:
    listener = OFListener(
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    true_command = f"rm -rf {decomposed_case_dir}/{timestamp}"
    generated_command = listener._create_delete_reconstructed_task(
        timestamp
    ).command
    assert generated_command == true_command


@pytest.mark.parametrize("timestamp", TEST_TIMESTAMP_STRINGS)
def test_create_tar_task_command(
    decomposed_case_dir: Path, timestamp: str, cluster: Mock
) -> None:
    listener = OFListener(
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    reconstruction_done_marker_filepath = (
        decomposed_case_dir / timestamp / RECONSTRUCTION_DONE_MARKER_FILENAME
    )
    true_command = (
        f"tar --exclude {reconstruction_done_marker_filepath}"
        f" -cvf {decomposed_case_dir}/{timestamp}.tar.inprogress"
        f" {decomposed_case_dir}/{timestamp}"
        f" &&"
        f" mv {decomposed_case_dir}/{timestamp}.tar.inprogress"
        f" {decomposed_case_dir}/{timestamp}.tar"
    )
    generated_command = listener._create_tar_task(timestamp).command
    assert generated_command == true_command
