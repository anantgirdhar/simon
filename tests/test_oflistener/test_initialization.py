from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from simon.oflistener import OFListener


def test_in_valid_provided_case_dir(
    decomposed_case_dir: Path, cluster: Mock
) -> None:
    # This should not raise an error
    OFListener(
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )


@pytest.mark.parametrize("leave_out_dir", ["constant", "system", "processor0"])
def test_in_bad_case_dir(
    tmp_path: Path, leave_out_dir: str, cluster: Mock
) -> None:
    # Setup the test case directory structure
    case_dir = tmp_path
    for directory in ["constant", "system", "processor0"]:
        if directory == leave_out_dir:
            # Don't make this directory
            continue
        (case_dir / directory).mkdir()
    with pytest.raises(ValueError):
        OFListener(
            keep_every=Decimal("0.0001"),
            compress_every=Decimal("0.01"),
            cluster=cluster,
            case_dir=case_dir,
        )
