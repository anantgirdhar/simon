from decimal import Decimal
from unittest.mock import Mock

import pytest
from simon.openfoam.listener import OFListener


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.1"), Decimal("0.01")),
        (Decimal("0.1"), Decimal("0.02")),
        (Decimal("0.1"), Decimal("0.03")),
        (Decimal("0.1"), Decimal("0.04")),
        (Decimal("0.1"), Decimal("0.05")),
        (Decimal("0.1"), Decimal("0.06")),
        (Decimal("0.1"), Decimal("0.07")),
        (Decimal("0.1"), Decimal("0.08")),
        (Decimal("0.1"), Decimal("0.09")),
        (Decimal("1"), Decimal("0.01")),
        (Decimal("1"), Decimal("0.02")),
        (Decimal("1"), Decimal("0.03")),
        (Decimal("1"), Decimal("0.08")),
        (Decimal("1"), Decimal("0.09")),
        (Decimal("1"), Decimal("0.1")),
        (Decimal("1"), Decimal("0.2")),
        (Decimal("1"), Decimal("0.3")),
        (Decimal("1"), Decimal("0.8")),
        (Decimal("1"), Decimal("0.9")),
        (Decimal("10"), Decimal("0.01")),
        (Decimal("10"), Decimal("0.02")),
        (Decimal("10"), Decimal("0.03")),
        (Decimal("10"), Decimal("0.08")),
        (Decimal("10"), Decimal("0.09")),
        (Decimal("10"), Decimal("0.1")),
        (Decimal("10"), Decimal("0.2")),
        (Decimal("10"), Decimal("0.3")),
        (Decimal("10"), Decimal("0.8")),
        (Decimal("10"), Decimal("0.9")),
        (Decimal("10"), Decimal("1")),
        (Decimal("10"), Decimal("1.1")),
        (Decimal("10"), Decimal("1.2")),
        (Decimal("10"), Decimal("1.3")),
        (Decimal("10"), Decimal("1.8")),
        (Decimal("10"), Decimal("1.9")),
        (Decimal("10"), Decimal("2")),
        (Decimal("10"), Decimal("3")),
        (Decimal("10"), Decimal("8")),
        (Decimal("10"), Decimal("9")),
        (Decimal("0.3"), Decimal("0.01")),
        (Decimal("0.3"), Decimal("0.02")),
        (Decimal("0.3"), Decimal("0.03")),
        (Decimal("0.3"), Decimal("0.04")),
        (Decimal("0.3"), Decimal("0.05")),
        (Decimal("0.3"), Decimal("0.06")),
        (Decimal("0.3"), Decimal("0.07")),
        (Decimal("0.3"), Decimal("0.08")),
        (Decimal("0.3"), Decimal("0.09")),
        (Decimal("3"), Decimal("0.01")),
        (Decimal("3"), Decimal("0.02")),
        (Decimal("3"), Decimal("0.03")),
        (Decimal("3"), Decimal("0.08")),
        (Decimal("3"), Decimal("0.09")),
        (Decimal("3"), Decimal("0.1")),
        (Decimal("3"), Decimal("0.2")),
        (Decimal("3"), Decimal("0.3")),
        (Decimal("3"), Decimal("0.8")),
        (Decimal("3"), Decimal("0.9")),
        (Decimal("30"), Decimal("0.01")),
        (Decimal("30"), Decimal("0.02")),
        (Decimal("30"), Decimal("0.03")),
        (Decimal("30"), Decimal("0.08")),
        (Decimal("30"), Decimal("0.09")),
        (Decimal("30"), Decimal("0.1")),
        (Decimal("30"), Decimal("0.2")),
        (Decimal("30"), Decimal("0.3")),
        (Decimal("30"), Decimal("0.8")),
        (Decimal("30"), Decimal("0.9")),
        (Decimal("30"), Decimal("1")),
        (Decimal("30"), Decimal("1.1")),
        (Decimal("30"), Decimal("1.2")),
        (Decimal("30"), Decimal("1.3")),
        (Decimal("30"), Decimal("1.8")),
        (Decimal("30"), Decimal("1.9")),
        (Decimal("30"), Decimal("2")),
        (Decimal("30"), Decimal("3")),
        (Decimal("30"), Decimal("8")),
        (Decimal("30"), Decimal("9")),
    ],
)
def test_raises_value_error_when_compress_every_is_made_smaller_than_keep_every(
    keep_every: Decimal, compress_every: Decimal
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=keep_every,
        compress_every=10 * keep_every,
        cluster=Mock(),
    )
    with pytest.raises(ValueError):
        listener.compress_every = compress_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.1"), Decimal("0.01")),
        (Decimal("0.1"), Decimal("0.02")),
        (Decimal("0.1"), Decimal("0.03")),
        (Decimal("0.1"), Decimal("0.04")),
        (Decimal("0.1"), Decimal("0.05")),
        (Decimal("0.1"), Decimal("0.06")),
        (Decimal("0.1"), Decimal("0.07")),
        (Decimal("0.1"), Decimal("0.08")),
        (Decimal("0.1"), Decimal("0.09")),
        (Decimal("1"), Decimal("0.01")),
        (Decimal("1"), Decimal("0.02")),
        (Decimal("1"), Decimal("0.03")),
        (Decimal("1"), Decimal("0.08")),
        (Decimal("1"), Decimal("0.09")),
        (Decimal("1"), Decimal("0.1")),
        (Decimal("1"), Decimal("0.2")),
        (Decimal("1"), Decimal("0.3")),
        (Decimal("1"), Decimal("0.8")),
        (Decimal("1"), Decimal("0.9")),
        (Decimal("10"), Decimal("0.01")),
        (Decimal("10"), Decimal("0.02")),
        (Decimal("10"), Decimal("0.03")),
        (Decimal("10"), Decimal("0.08")),
        (Decimal("10"), Decimal("0.09")),
        (Decimal("10"), Decimal("0.1")),
        (Decimal("10"), Decimal("0.2")),
        (Decimal("10"), Decimal("0.3")),
        (Decimal("10"), Decimal("0.8")),
        (Decimal("10"), Decimal("0.9")),
        (Decimal("10"), Decimal("1")),
        (Decimal("10"), Decimal("1.1")),
        (Decimal("10"), Decimal("1.2")),
        (Decimal("10"), Decimal("1.3")),
        (Decimal("10"), Decimal("1.8")),
        (Decimal("10"), Decimal("1.9")),
        (Decimal("10"), Decimal("2")),
        (Decimal("10"), Decimal("3")),
        (Decimal("10"), Decimal("8")),
        (Decimal("10"), Decimal("9")),
        (Decimal("0.3"), Decimal("0.01")),
        (Decimal("0.3"), Decimal("0.02")),
        (Decimal("0.3"), Decimal("0.03")),
        (Decimal("0.3"), Decimal("0.04")),
        (Decimal("0.3"), Decimal("0.05")),
        (Decimal("0.3"), Decimal("0.06")),
        (Decimal("0.3"), Decimal("0.07")),
        (Decimal("0.3"), Decimal("0.08")),
        (Decimal("0.3"), Decimal("0.09")),
        (Decimal("3"), Decimal("0.01")),
        (Decimal("3"), Decimal("0.02")),
        (Decimal("3"), Decimal("0.03")),
        (Decimal("3"), Decimal("0.08")),
        (Decimal("3"), Decimal("0.09")),
        (Decimal("3"), Decimal("0.1")),
        (Decimal("3"), Decimal("0.2")),
        (Decimal("3"), Decimal("0.3")),
        (Decimal("3"), Decimal("0.8")),
        (Decimal("3"), Decimal("0.9")),
        (Decimal("30"), Decimal("0.01")),
        (Decimal("30"), Decimal("0.02")),
        (Decimal("30"), Decimal("0.03")),
        (Decimal("30"), Decimal("0.08")),
        (Decimal("30"), Decimal("0.09")),
        (Decimal("30"), Decimal("0.1")),
        (Decimal("30"), Decimal("0.2")),
        (Decimal("30"), Decimal("0.3")),
        (Decimal("30"), Decimal("0.8")),
        (Decimal("30"), Decimal("0.9")),
        (Decimal("30"), Decimal("1")),
        (Decimal("30"), Decimal("1.1")),
        (Decimal("30"), Decimal("1.2")),
        (Decimal("30"), Decimal("1.3")),
        (Decimal("30"), Decimal("1.8")),
        (Decimal("30"), Decimal("1.9")),
        (Decimal("30"), Decimal("2")),
        (Decimal("30"), Decimal("3")),
        (Decimal("30"), Decimal("8")),
        (Decimal("30"), Decimal("9")),
    ],
)
def test_raises_value_error_when_keep_every_is_made_larger_than_compress_every(
    keep_every: Decimal, compress_every: Decimal
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=Decimal("0.1") * compress_every,
        compress_every=compress_every,
        cluster=Mock(),
    )
    with pytest.raises(ValueError):
        listener.keep_every = keep_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.01"), Decimal("0.01")),
        (Decimal("0.1"), Decimal("0.1")),
        (Decimal("1"), Decimal("1")),
        (Decimal("10"), Decimal("10")),
        (Decimal("0.02"), Decimal("0.02")),
        (Decimal("0.2"), Decimal("0.2")),
        (Decimal("2"), Decimal("2")),
        (Decimal("20"), Decimal("20")),
        (Decimal("0.03"), Decimal("0.03")),
        (Decimal("0.3"), Decimal("0.3")),
        (Decimal("3"), Decimal("3")),
        (Decimal("30"), Decimal("30")),
    ],
)
def test_raises_value_error_when_compress_every_is_made_equal_to_keep_every(
    keep_every: Decimal,
    compress_every: Decimal,
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=keep_every,
        compress_every=10 * keep_every,
        cluster=Mock(),
    )
    with pytest.raises(ValueError):
        listener.compress_every = compress_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.01"), Decimal("0.01")),
        (Decimal("0.1"), Decimal("0.1")),
        (Decimal("1"), Decimal("1")),
        (Decimal("10"), Decimal("10")),
        (Decimal("0.02"), Decimal("0.02")),
        (Decimal("0.2"), Decimal("0.2")),
        (Decimal("2"), Decimal("2")),
        (Decimal("20"), Decimal("20")),
        (Decimal("0.03"), Decimal("0.03")),
        (Decimal("0.3"), Decimal("0.3")),
        (Decimal("3"), Decimal("3")),
        (Decimal("30"), Decimal("30")),
    ],
)
def test_raises_value_error_when_keep_every_is_made_equal_to_compress_every(
    keep_every: Decimal,
    compress_every: Decimal,
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=Decimal("0.1") * compress_every,
        compress_every=compress_every,
        cluster=Mock(),
    )
    with pytest.raises(ValueError):
        listener.keep_every = keep_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.01"), Decimal("0.02")),
        (Decimal("0.01"), Decimal("0.03")),
        (Decimal("0.01"), Decimal("0.04")),
        (Decimal("0.01"), Decimal("0.05")),
        (Decimal("0.01"), Decimal("0.06")),
        (Decimal("0.01"), Decimal("0.1")),
        (Decimal("0.01"), Decimal("0.2")),
        (Decimal("0.01"), Decimal("0.3")),
        (Decimal("0.01"), Decimal("0.5")),
        (Decimal("0.01"), Decimal("0.6")),
        (Decimal("0.01"), Decimal("1")),
        (Decimal("0.01"), Decimal("2")),
        (Decimal("0.01"), Decimal("3")),
        (Decimal("0.01"), Decimal("5")),
        (Decimal("1"), Decimal("2")),
        (Decimal("1"), Decimal("3")),
        (Decimal("1"), Decimal("4")),
        (Decimal("1"), Decimal("5")),
        (Decimal("1"), Decimal("6")),
        (Decimal("1"), Decimal("10")),
        (Decimal("1"), Decimal("30")),
        (Decimal("1"), Decimal("100")),
        (Decimal("1"), Decimal("300")),
        (Decimal("0.03"), Decimal("0.06")),
        (Decimal("0.03"), Decimal("0.3")),
        (Decimal("0.03"), Decimal("0.6")),
        (Decimal("0.03"), Decimal("3")),
        (Decimal("0.03"), Decimal("6")),
        (Decimal("3"), Decimal("6")),
        (Decimal("3"), Decimal("30")),
        (Decimal("3"), Decimal("60")),
        (Decimal("3"), Decimal("300")),
        (Decimal("3"), Decimal("900")),
    ],
)
def test_initializes_successfully_when_keep_every_is_updated_correctly(
    keep_every: Decimal, compress_every: Decimal
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=Decimal("0.1") * compress_every,
        compress_every=compress_every,
        cluster=Mock(),
    )
    listener.keep_every = keep_every
    assert listener.keep_every == keep_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.01"), Decimal("0.02")),
        (Decimal("0.01"), Decimal("0.03")),
        (Decimal("0.01"), Decimal("0.04")),
        (Decimal("0.01"), Decimal("0.05")),
        (Decimal("0.01"), Decimal("0.06")),
        (Decimal("0.01"), Decimal("0.1")),
        (Decimal("0.01"), Decimal("0.2")),
        (Decimal("0.01"), Decimal("0.3")),
        (Decimal("0.01"), Decimal("0.5")),
        (Decimal("0.01"), Decimal("0.6")),
        (Decimal("0.01"), Decimal("1")),
        (Decimal("0.01"), Decimal("2")),
        (Decimal("0.01"), Decimal("3")),
        (Decimal("0.01"), Decimal("5")),
        (Decimal("1"), Decimal("2")),
        (Decimal("1"), Decimal("3")),
        (Decimal("1"), Decimal("4")),
        (Decimal("1"), Decimal("5")),
        (Decimal("1"), Decimal("6")),
        (Decimal("1"), Decimal("10")),
        (Decimal("1"), Decimal("30")),
        (Decimal("1"), Decimal("100")),
        (Decimal("1"), Decimal("300")),
        (Decimal("0.03"), Decimal("0.06")),
        (Decimal("0.03"), Decimal("0.3")),
        (Decimal("0.03"), Decimal("0.6")),
        (Decimal("0.03"), Decimal("3")),
        (Decimal("0.03"), Decimal("6")),
        (Decimal("3"), Decimal("6")),
        (Decimal("3"), Decimal("30")),
        (Decimal("3"), Decimal("60")),
        (Decimal("3"), Decimal("300")),
        (Decimal("3"), Decimal("900")),
    ],
)
def test_initializes_successfully_when_compress_every_is_updated_correctly(
    keep_every: Decimal, compress_every: Decimal
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=keep_every,
        compress_every=10 * keep_every,
        cluster=Mock(),
    )
    listener.compress_every = compress_every
    assert listener.compress_every == compress_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.01"), Decimal("0.015")),
        (Decimal("0.01"), Decimal("0.025")),
        (Decimal("0.02"), Decimal("0.05")),
        (Decimal("1"), Decimal("1.05")),
        (Decimal("1"), Decimal("1.5")),
        (Decimal("1"), Decimal("1.999999")),
        (Decimal("2"), Decimal("3")),
        (Decimal("2"), Decimal("5")),
        (Decimal("2"), Decimal("6.000000001")),
        (Decimal("0.03"), Decimal("0.04")),
        (Decimal("0.03"), Decimal("0.05")),
        (Decimal("0.03"), Decimal("0.1")),
        (Decimal("0.03"), Decimal("0.2")),
        (Decimal("0.03"), Decimal("1")),
        (Decimal("0.03"), Decimal("2")),
        (Decimal("3"), Decimal("4")),
        (Decimal("3"), Decimal("5")),
        (Decimal("3"), Decimal("10")),
        (Decimal("3"), Decimal("20")),
    ],
)
def test_raises_value_error_when_keep_every_is_updated_and_does_not_divide_compress_every(
    keep_every: Decimal, compress_every: Decimal
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=Decimal("0.1") * compress_every,
        compress_every=compress_every,
        cluster=Mock(),
    )
    with pytest.raises(ValueError):
        listener.keep_every = keep_every


@pytest.mark.parametrize(
    "keep_every, compress_every",
    [
        (Decimal("0.01"), Decimal("0.015")),
        (Decimal("0.01"), Decimal("0.025")),
        (Decimal("0.02"), Decimal("0.05")),
        (Decimal("1"), Decimal("1.05")),
        (Decimal("1"), Decimal("1.5")),
        (Decimal("1"), Decimal("1.999999")),
        (Decimal("2"), Decimal("3")),
        (Decimal("2"), Decimal("5")),
        (Decimal("2"), Decimal("6.000000001")),
        (Decimal("0.03"), Decimal("0.04")),
        (Decimal("0.03"), Decimal("0.05")),
        (Decimal("0.03"), Decimal("0.1")),
        (Decimal("0.03"), Decimal("0.2")),
        (Decimal("0.03"), Decimal("1")),
        (Decimal("0.03"), Decimal("2")),
        (Decimal("3"), Decimal("4")),
        (Decimal("3"), Decimal("5")),
        (Decimal("3"), Decimal("10")),
        (Decimal("3"), Decimal("20")),
    ],
)
def test_raises_value_error_when_compress_every_is_updated_and_does_not_divide_keep_every(
    keep_every: Decimal, compress_every: Decimal
) -> None:
    listener = OFListener(
        state=Mock(),
        keep_every=keep_every,
        compress_every=10 * keep_every,
        cluster=Mock(),
    )
    with pytest.raises(ValueError):
        listener.compress_every = compress_every
