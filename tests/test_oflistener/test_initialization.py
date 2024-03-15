from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import pytest
from simon.openfoam.file_state import OFFileState
from simon.openfoam.listener import OFListener


def test_in_valid_provided_case_dir(
    decomposed_case_dir: Path, cluster: Mock
) -> None:
    # This should not raise an error
    OFListener(
        state=OFFileState(decomposed_case_dir),
        keep_every=Decimal("0.0001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
    )


@pytest.mark.parametrize("leave_out_dir", ["constant", "system", "processor0"])
def test_in_bad_case_dir(
    decomposed_case_dir: Path,
    leave_out_dir: str,
    cluster: Mock,
) -> None:
    # Delete the leave_out_dir to simulate this condition
    (decomposed_case_dir / leave_out_dir).rmdir()
    with pytest.raises(ValueError):
        OFListener(
            state=OFFileState(decomposed_case_dir),
            keep_every=Decimal("0.0001"),
            compress_every=Decimal("0.01"),
            cluster=cluster,
        )
