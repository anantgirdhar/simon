from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from simon.oflistener import OFListener
from tests.test_oflistener.conftest import TEST_TIMESTAMP_DECIMALS


# See pytest_generate_tests for test data generation
def test_should_delete_smaller_timestamps(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    timestamp: Decimal,
    cluster: Mock,
) -> None:
    # Anything smaller than keep_every should be deleted
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    assert listener._delete_without_processing(timestamp) is True


@pytest.mark.parametrize("timestamp", TEST_TIMESTAMP_DECIMALS)
def test_should_not_delete_equal_timestamps(
    decomposed_case_dir: Path, timestamp: Decimal, cluster: Mock
) -> None:
    listener = OFListener(
        keep_every=timestamp,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    assert listener._delete_without_processing(timestamp) is False


@pytest.mark.parametrize(
    "keep_every, timestamp, result",
    [
        (Decimal("0.1"), Decimal("0.3"), False),
        (Decimal("0.1"), Decimal("0.5"), False),
        (Decimal("0.1"), Decimal("1"), False),
        (Decimal("0.1"), Decimal("3"), False),
        (Decimal("0.1"), Decimal("5"), False),
        (Decimal("0.1"), Decimal("10"), False),
        (Decimal("0.3"), Decimal("0.5"), True),
        (Decimal("0.3"), Decimal("1"), True),
        (Decimal("0.3"), Decimal("3"), False),
        (Decimal("0.3"), Decimal("5"), True),
        (Decimal("0.3"), Decimal("10"), True),
        (Decimal("0.5"), Decimal("1"), False),
        (Decimal("0.5"), Decimal("3"), False),
        (Decimal("0.5"), Decimal("5"), False),
        (Decimal("0.5"), Decimal("10"), False),
        (Decimal("1"), Decimal("3"), False),
        (Decimal("1"), Decimal("5"), False),
        (Decimal("1"), Decimal("10"), False),
        (Decimal("3"), Decimal("5"), True),
        (Decimal("3"), Decimal("10"), True),
        (Decimal("5"), Decimal("10"), False),
    ],
)
def test_delete_larger_timestamps(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    timestamp: Decimal,
    result: bool,
    cluster: Mock,
) -> None:
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    assert listener._delete_without_processing(timestamp) is result
