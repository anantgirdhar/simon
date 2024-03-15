from decimal import Decimal
from pathlib import Path
from typing import List
from unittest.mock import Mock

import pytest
from simon.oflistener import RECONSTRUCTION_DONE_MARKER_FILENAME, OFListener

from tests._oflistener_test_cases import OFLISTENER_TEST_CASES_SPLIT_ONLY

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


# Fixtures to create some timestamps


def pytest_generate_tests(metafunc) -> None:  # type: ignore
    if metafunc.function.__name__ == "test_should_delete_smaller_timestamps":
        # Create tests where the timestamp being tested is smaller than
        # keep_every to check if each of them is being deleted
        test_data = []
        for t in TEST_TIMESTAMP_DECIMALS:
            for smaller_t in TEST_TIMESTAMP_DECIMALS:
                if smaller_t < t:
                    test_data.append((t, smaller_t))
        metafunc.parametrize("keep_every, timestamp", test_data)


# Case directory setup convenience functions


@pytest.fixture
def decomposed_case_dir(tmp_path: Path) -> Path:
    case_dir = tmp_path
    (case_dir / "constant").mkdir()
    (case_dir / "system").mkdir()
    for i in range(NUM_PROCESSORS):
        (case_dir / f"processor{i}").mkdir()
    return case_dir


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


@pytest.fixture
def cluster() -> Mock:
    return Mock(spec=["requeue_job", "compress"])


# Test directory check during initialization


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


# Test timestamp deletion check


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


# Test commands are correct and items are getting substituted in correctly


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
    true_command = f"reconstructPar -time {timestamp} -case {decomposed_case_dir} && touch {reconstruction_done_marker_filepath}"
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


# Test reconstruction tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_reconstruct_new_times(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't reconstruct the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "reconstruct":
            required_reconstruct_task = listener._create_reconstruct_task(
                timestamp
            )
            assert required_reconstruct_task in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_reconstruct_last_time(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_reconstruct_task(split_times[-1])
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "num_reconstructed_dirs",
    range(1, len(OFLISTENER_TEST_CASES_SPLIT_ONLY[0][1])),
)
@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_reconstruct_already_reconstructed_times(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    num_reconstructed_dirs: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Reconstruct as many directories as requested by the test
    already_reconstructed_times = split_times[:num_reconstructed_dirs]
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, already_reconstructed_times
    )
    tasks = listener.get_new_tasks()
    for timestamp in already_reconstructed_times:
        unallowed_task = listener._create_reconstruct_task(timestamp)
        assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_reconstruct_partially_reconstructed_times(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Partially reconstruct every timestamp
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, split_times
    )
    tasks = listener.get_new_tasks()
    # Now, every timestamp that had a reconstruct action originally should
    # still be getting reconstructed
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't reconstruct the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "reconstruct":
            required_reconstruct_task = listener._create_reconstruct_task(
                timestamp
            )
            assert required_reconstruct_task in tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(OFLISTENER_TEST_CASES_SPLIT_ONLY[0][1])),
)
@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_reconstruct_tarred_times(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Tar as many directories as requested by the test
    already_tarred_times = split_times[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in already_tarred_times:
        unallowed_task = listener._create_reconstruct_task(timestamp)
        assert unallowed_task not in tasks


# Test delete split tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_deletes_split_times_if_not_wanted(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't delete the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "delete":
            required_task = listener._create_delete_split_task(timestamp)
            assert required_task in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_not_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(split_times[-1])
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_partially_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    last_split_time = split_times[-1]
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, [last_split_time]
    )
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(last_split_time)
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_fully_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    last_split_time = split_times[-1]
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, [last_split_time]
    )
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(last_split_time)
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_last_split_time_when_it_is_tarred(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    last_split_time = split_times[-1]
    create_reconstructed_tars(decomposed_case_dir, [last_split_time])
    tasks = listener.get_new_tasks()
    unallowed_task = listener._create_delete_split_task(last_split_time)
    assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "num_reconstructed_dirs",
    range(1, len(OFLISTENER_TEST_CASES_SPLIT_ONLY[0][1])),
)
@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_deletes_split_times_if_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    num_reconstructed_dirs: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Reconstruct as many directories as requested by the test
    already_reconstructed_times = split_times[:num_reconstructed_dirs]
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, already_reconstructed_times
    )
    tasks = listener.get_new_tasks()
    for timestamp in already_reconstructed_times:
        required_task = listener._create_delete_split_task(timestamp)
        assert required_task in tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(OFLISTENER_TEST_CASES_SPLIT_ONLY[0][1])),
)
@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_deletes_split_times_if_tarred(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Tar as many directories as requested by the test
    already_tarred_times = split_times[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in already_tarred_times:
        required_task = listener._create_delete_split_task(timestamp)
        assert required_task in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_split_time_if_not_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't delete the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "reconstruct":
            unallowed_task = listener._create_delete_split_task(timestamp)
            assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "keep_every, split_times, actions", OFLISTENER_TEST_CASES_SPLIT_ONLY
)
def test_does_not_delete_split_time_if_partially_reconstructed(
    decomposed_case_dir: Path,
    keep_every: Decimal,
    split_times: List[str],
    actions: List[str],
    cluster: Mock,
) -> None:
    # Setup the fake case with fake split data
    listener = OFListener(
        keep_every=keep_every,
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_split_timestamps(decomposed_case_dir, split_times)
    # Partially reconstruct every timestamp
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, split_times
    )
    tasks = listener.get_new_tasks()
    for timestamp, action in zip(split_times, actions):
        if timestamp == split_times[-1]:
            # Don't delete the last timestamp because we don't know if it
            # has been written out completely
            continue
        if action == "reconstruct":
            unallowed_task = listener._create_delete_split_task(timestamp)
            assert unallowed_task not in tasks


# Test delete reconstructed tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_deletes_reconstructed_times_if_tarred(
    decomposed_case_dir: Path,
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, TEST_TIMESTAMP_STRINGS
    )
    # Tar as many directories as requested by the test
    already_tarred_times = TEST_TIMESTAMP_STRINGS[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in already_tarred_times:
        required_task = listener._create_delete_reconstructed_task(timestamp)
        assert required_task in tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_does_not_delete_reconstructed_times_if_not_tarred(
    decomposed_case_dir: Path,
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, TEST_TIMESTAMP_STRINGS
    )
    # Tar as many directories as requested by the test
    already_tarred_times = TEST_TIMESTAMP_STRINGS[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        if timestamp in already_tarred_times:
            continue
        # If a time is not tarred, it should not be deleted
        unallowed_task = listener._create_delete_reconstructed_task(timestamp)
        assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "num_partially_reconstructed_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_does_not_delete_partially_reconstructed_times(
    decomposed_case_dir: Path,
    num_partially_reconstructed_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    # Partially reconstruct as many times as requested by the test
    partial_times = TEST_TIMESTAMP_STRINGS[:num_partially_reconstructed_times]
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, partial_times
    )
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        if timestamp in partial_times:
            # If a time is partially reconstructed it should not be deleted
            unallowed_task = listener._create_delete_reconstructed_task(
                timestamp
            )
            assert unallowed_task not in tasks


# Test tar tasks


def test_tars_times_if_reconstructed(
    decomposed_case_dir: Path,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, TEST_TIMESTAMP_STRINGS
    )
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        required_task = listener._create_tar_task(timestamp)
        assert required_task in tasks


@pytest.mark.parametrize(
    "num_partially_reconstructed_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_does_not_tar_time_if_partially_reconstructed(
    decomposed_case_dir: Path,
    num_partially_reconstructed_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    # Partially reconstruct as many times as requested by the test
    partial_times = TEST_TIMESTAMP_STRINGS[:num_partially_reconstructed_times]
    create_reconstructed_timestamps_without_done_marker(
        decomposed_case_dir, partial_times
    )
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        if timestamp in partial_times:
            # If a time is partially reconstructed it should not be deleted
            unallowed_task = listener._create_tar_task(timestamp)
            assert unallowed_task not in tasks


@pytest.mark.parametrize(
    "num_tarred_times",
    range(1, len(TEST_TIMESTAMP_STRINGS)),
)
def test_does_not_tar_time_if_already_tarred(
    decomposed_case_dir: Path,
    num_tarred_times: int,
    cluster: Mock,
) -> None:
    # Setup the fake case with fake reconstructed data
    listener = OFListener(
        keep_every=Decimal("0.001"),
        compress_every=Decimal("0.01"),
        cluster=cluster,
        case_dir=decomposed_case_dir,
    )
    create_reconstructed_timestamps_with_done_marker(
        decomposed_case_dir, TEST_TIMESTAMP_STRINGS
    )
    # Tar as many directories as requested by the test
    already_tarred_times = TEST_TIMESTAMP_STRINGS[:num_tarred_times]
    create_reconstructed_tars(decomposed_case_dir, already_tarred_times)
    tasks = listener.get_new_tasks()
    for timestamp in TEST_TIMESTAMP_STRINGS:
        if timestamp in already_tarred_times:
            # If a time is tarred, it should not be tarred again
            unallowed_task = listener._create_tar_task(timestamp)
            assert unallowed_task not in tasks
