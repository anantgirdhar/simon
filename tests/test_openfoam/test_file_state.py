from pathlib import Path

import pytest
from simon.openfoam.file_state import OFFileState

# Test initialization


def test_in_valid_provided_case_dir(decomposed_case_dir: Path) -> None:
    # This should not raise an error
    OFFileState(decomposed_case_dir)


@pytest.mark.parametrize("leave_out_dir", ["constant", "system", "processor0"])
def test_in_bad_case_dir(
    decomposed_case_dir: Path,
    leave_out_dir: str,
) -> None:
    # Delete the leave_out_dir to simulate this condition
    (decomposed_case_dir / leave_out_dir).rmdir()
    with pytest.raises(ValueError):
        OFFileState(decomposed_case_dir)
