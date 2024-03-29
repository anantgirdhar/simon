from decimal import Decimal
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from simon.openfoam.file_state import (RECONSTRUCTION_DONE_MARKER_FILENAME,
                                       OFFileState)
from simon.openfoam.listener import OFListener

TEST_VARIABLES = ["U", "T", "p", "H2"]

NUM_PROCESSORS = 16
TEST_TIMESTAMP_STRINGS = [
    "0.001",
    "0.003",
    "0.005",
    "0.01",
    "0.03",
    "0.05",
    "0.1",
    "0.3",
    "0.5",
    "1",
    "3",
    "5",
    "10",
]
TEST_TIMESTAMP_DECIMALS = list(
    sorted(set(Decimal(t) for t in TEST_TIMESTAMP_STRINGS))
)


# Case directory setup convenience functions


def create_split_timestamps(case_dir: Path, timestamps: List[str]) -> None:
    for i in range(NUM_PROCESSORS):
        for timestamp in timestamps:
            (case_dir / f"processor{i}" / timestamp).mkdir()
            for var in TEST_VARIABLES:
                (case_dir / f"processor{i}" / timestamp / var).touch()


def create_reconstructed_timestamps_without_done_marker(
    case_dir: Path, timestamps: List[str]
) -> None:
    for timestamp in timestamps:
        (case_dir / timestamp).mkdir()
        for var in TEST_VARIABLES:
            (case_dir / timestamp / var).touch()


def create_reconstructed_timestamps_with_done_marker(
    case_dir: Path, timestamps: List[str]
) -> None:
    create_reconstructed_timestamps_without_done_marker(case_dir, timestamps)
    for timestamp in timestamps:
        (case_dir / timestamp / RECONSTRUCTION_DONE_MARKER_FILENAME).touch()


def create_reconstructed_tars(case_dir: Path, timestamps: List[str]) -> None:
    for timestamp in timestamps:
        (case_dir / f"{timestamp}.tar").touch()


def create_compressed_files(case_dir: Path, filenames: List[str]) -> None:
    for filename in filenames:
        (case_dir / filename).touch()


@pytest.fixture
def decomposed_case_dir(tmp_path: Path) -> Path:
    case_dir = tmp_path
    (case_dir / "constant").mkdir()
    (case_dir / "system").mkdir()
    for i in range(NUM_PROCESSORS):
        (case_dir / f"processor{i}").mkdir()
    return case_dir


@pytest.fixture
def listener(decomposed_case_dir: Path) -> OFListener:
    return OFListener(
        state=OFFileState(decomposed_case_dir),
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("3000"),
        cluster=Mock(spec=["requeue_job", "compress"]),
    )


@pytest.fixture
def times() -> List[str]:
    return sorted(TEST_TIMESTAMP_STRINGS, key=float)
