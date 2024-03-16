import shutil
from pathlib import Path
from typing import Callable, List

import pytest
from simon.openfoam.file_state import OFFileState
from tests.test_openfoam.conftest import (
    create_reconstructed_tars,
    create_reconstructed_timestamps_with_done_marker,
    create_reconstructed_timestamps_without_done_marker,
    create_split_timestamps)


@pytest.fixture
def state(decomposed_case_dir: Path) -> OFFileState:
    return OFFileState(decomposed_case_dir)


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


# Test get file state


def test_given_only_split_times_returns_correct_list_of_split_times(
    state: OFFileState, times: List[str]
) -> None:
    create_split_timestamps(state.case_dir, times)
    assert state.get_split_times() == times


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_timestamps_with_done_marker,
        create_reconstructed_tars,
    ],
)
@pytest.mark.parametrize("delete_split_time", ["0.001", "0.03", "10"])
def test_given_extra_file_states_returns_correct_list_of_split_times(
    state: OFFileState,
    times: List[str],
    delete_split_time: str,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    times.remove(delete_split_time)
    create_split_timestamps(state.case_dir, times)
    create_extra_files(state.case_dir, ["0.001", "0.03", "10"])
    assert state.get_split_times() == times


def test_given_only_reconstructed_times_returns_correct_list_of_reconstructed_times(
    state: OFFileState, times: List[str]
) -> None:
    create_reconstructed_timestamps_with_done_marker(state.case_dir, times)
    assert state.get_reconstructed_times() == times


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_tars,
    ],
)
@pytest.mark.parametrize("delete_reconstructed_time", ["0.001", "0.03", "10"])
def test_given_extra_file_states_returns_correct_list_of_reconstructed_times(
    state: OFFileState,
    times: List[str],
    delete_reconstructed_time: str,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    times.remove(delete_reconstructed_time)
    create_reconstructed_timestamps_with_done_marker(state.case_dir, times)
    create_extra_files(state.case_dir, [delete_reconstructed_time])
    assert state.get_reconstructed_times() == times


def test_given_only_tarred_times_returns_correct_list_of_reconstructed_times(
    state: OFFileState, times: List[str]
) -> None:
    create_reconstructed_tars(state.case_dir, times)
    assert state.get_tarred_times() == times


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_timestamps_with_done_marker,
    ],
)
@pytest.mark.parametrize("delete_tarred_time", ["0.001", "0.03", "10"])
def test_given_extra_file_states_returns_correct_list_of_tarred_times(
    state: OFFileState,
    times: List[str],
    delete_tarred_time: str,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    times.remove(delete_tarred_time)
    create_reconstructed_tars(state.case_dir, times)
    create_extra_files(state.case_dir, ["0.001", "0.03", "10"])
    assert state.get_tarred_times() == times


def test_given_only_partially_reconstructed_times_does_not_return_anything(
    state: OFFileState, times: List[str]
) -> None:
    create_reconstructed_timestamps_without_done_marker(state.case_dir, times)
    assert state.get_split_times() == []
    assert state.get_reconstructed_times() == []
    assert state.get_tarred_times() == []


# Test query timestamp properties


def test_is_reconstructed_returns_true_when_is_reconstructed(
    state: OFFileState,
) -> None:
    create_reconstructed_timestamps_with_done_marker(state.case_dir, ["0.1"])
    assert state.is_reconstructed("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_tars,
    ],
)
def test_is_reconstructed_returns_true_even_when_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_reconstructed_timestamps_with_done_marker(state.case_dir, ["0.1"])
    create_extra_files(state.case_dir, ["0.1"])
    assert state.is_reconstructed("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_tars,
    ],
)
def test_is_reconstructed_returns_false_when_only_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_extra_files(state.case_dir, ["0.1"])
    assert not state.is_reconstructed("0.1")


def test_is_tarred_returns_true_when_is_tarred(
    state: OFFileState,
) -> None:
    create_reconstructed_tars(state.case_dir, ["0.1"])
    assert state.is_tarred("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_timestamps_with_done_marker,
        create_reconstructed_timestamps_without_done_marker,
    ],
)
def test_is_tarred_returns_true_even_when_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_reconstructed_tars(state.case_dir, ["0.1"])
    create_extra_files(state.case_dir, ["0.1"])
    assert state.is_tarred("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_timestamps_with_done_marker,
    ],
)
def test_is_tarred_returns_false_when_only_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_extra_files(state.case_dir, ["0.1"])
    assert not state.is_tarred("0.1")


def test_reconstructed_dir_exists_returns_true_when_reconstructed_time_exists(
    state: OFFileState,
) -> None:
    create_reconstructed_timestamps_with_done_marker(state.case_dir, ["0.1"])
    assert state.reconstructed_dir_exists("0.1")


def test_reconstructed_dir_exists_returns_true_when_partialy_reconstructed_time_exists(
    state: OFFileState,
) -> None:
    create_reconstructed_timestamps_without_done_marker(
        state.case_dir, ["0.1"]
    )
    assert state.reconstructed_dir_exists("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_tars,
    ],
)
def test_reconstructed_dir_exists_returns_true_even_when_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_reconstructed_timestamps_without_done_marker(
        state.case_dir, ["0.1"]
    )
    create_extra_files(state.case_dir, ["0.1"])
    assert state.reconstructed_dir_exists("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_split_timestamps,
        create_reconstructed_tars,
    ],
)
def test_reconstructed_dir_exists_returns_false_when_only_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_extra_files(state.case_dir, ["0.1"])
    assert not state.reconstructed_dir_exists("0.1")


def test_split_exists_returns_true_when_split_exists_in_all_processor_directories(
    state: OFFileState,
) -> None:
    create_split_timestamps(state.case_dir, ["0.1"])
    assert state.split_exists("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_reconstructed_timestamps_with_done_marker,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_tars,
    ],
)
def test_split_exists_returns_true_even_when_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_split_timestamps(state.case_dir, ["0.1"])
    create_extra_files(state.case_dir, ["0.1"])
    assert state.split_exists("0.1")


@pytest.mark.parametrize(
    "create_extra_files",
    [
        create_reconstructed_timestamps_with_done_marker,
        create_reconstructed_timestamps_without_done_marker,
        create_reconstructed_tars,
    ],
)
def test_split_exists_returns_false_when_only_others_are_present(
    state: OFFileState,
    create_extra_files: Callable[[Path, List[str]], None],
) -> None:
    create_extra_files(state.case_dir, ["0.1"])
    assert not state.split_exists("0.1")


def test_split_exists_returns_true_even_if_not_in_processor0(
    state: OFFileState,
) -> None:
    create_split_timestamps(state.case_dir, ["0.1"])
    shutil.rmtree(state.case_dir / "processor0" / "0.1")
    assert state.split_exists("0.1")


def test_split_exists_returns_true_even_if_only_in_processor1(
    state: OFFileState,
) -> None:
    create_split_timestamps(state.case_dir, ["0.1"])
    for d in state.case_dir.glob("processor*"):
        if d.name == "processor1":
            continue
        shutil.rmtree(d / "0.1")
    assert state.split_exists("0.1")
