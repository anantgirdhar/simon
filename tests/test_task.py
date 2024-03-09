import time
from pathlib import Path

import pytest
from simon.task import Task

# Create some setup and teardown fixtures to setup a temporary working
# directory

TEST_FILE = "task_touch.test"
TEST_SLEEP_TIME = 2


@pytest.fixture()
def fixed_tmp_dir(tmp_path: Path) -> Path:
    # Creating this because I want to ensure that every Task created operates
    # within the same directory
    return tmp_path


@pytest.fixture
def basic_task(fixed_tmp_dir: Path) -> Task:
    # This will help test a "fast" basic Task
    temp_test_file = fixed_tmp_dir / TEST_FILE
    # Make sure that the test file does not exist
    if temp_test_file.is_file():
        temp_test_file.unlink()
    return Task(command=f"touch {temp_test_file}")


@pytest.fixture
def task_with_check(fixed_tmp_dir: Path) -> Task:
    # This will help test a Task that can perform additional checks
    temp_test_file = fixed_tmp_dir / TEST_FILE
    # Make sure that the test file does not exist
    if temp_test_file.is_file():
        temp_test_file.unlink()

    def _check_file_exists() -> bool:
        return temp_test_file.is_file()

    return Task(
        command=f"touch {temp_test_file}", completion_check=_check_file_exists
    )


@pytest.fixture
def task_with_sleep(fixed_tmp_dir: Path) -> Task:
    # This will help test a Task that takes a long time to complete
    temp_test_file = fixed_tmp_dir / TEST_FILE
    # Make sure that the test file does not exist
    if temp_test_file.is_file():
        temp_test_file.unlink()
    return Task(command=f"sleep {TEST_SLEEP_TIME} && touch {temp_test_file}")


# Test basic_task


def test_basic_task_runs(fixed_tmp_dir: Path, basic_task: Task) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    basic_task.run()
    time.sleep(0.2)  # Give it a little bit of time to finish
    # Make sure that the file exists now
    assert (fixed_tmp_dir / TEST_FILE).is_file() is True


def test_basic_task_is_complete_after_run(
    fixed_tmp_dir: Path, basic_task: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    basic_task.run()
    time.sleep(0.2)  # Give it a little bit of time to finish
    # Check that the Task is complete now
    assert basic_task.is_complete()


def test_basic_task_was_successful_after_run(
    fixed_tmp_dir: Path, basic_task: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    basic_task.run()
    time.sleep(0.2)  # Give it a little bit of time to finish
    # Check that the Task is complete now
    assert basic_task.was_successful()


def test_basic_task_not_complete_before_run(
    fixed_tmp_dir: Path, basic_task: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Check that the Task is not complete yet
    assert basic_task.is_complete() is False


def test_basic_task_not_successful_before_run(
    fixed_tmp_dir: Path, basic_task: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Check that the Task is not complete yet
    assert basic_task.was_successful() is None


def test_basic_task_not_complete_before_running_even_if_already_complete(
    fixed_tmp_dir: Path, basic_task: Task
) -> None:
    # Create the test file before we start to simulate the Task is complete
    (fixed_tmp_dir / TEST_FILE).touch()
    assert (fixed_tmp_dir / TEST_FILE).is_file()
    # At this point, the basic task has no way of checking if the Task is
    # already complete because we haven't provided it with an extra function to
    # check that
    assert basic_task.is_complete() is False


# Test task_with_check


def test_task_with_check_runs(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    task_with_check.run()
    time.sleep(0.2)  # Give it a little bit of time to finish
    # Make sure that the file exists now
    assert (fixed_tmp_dir / TEST_FILE).is_file() is True


def test_task_with_check_is_complete_after_run(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    task_with_check.run()
    time.sleep(0.2)  # Give it a little bit of time to finish
    # Check that the Task is complete now
    assert task_with_check.is_complete()


def test_task_with_check_was_successful_after_run(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    task_with_check.run()
    time.sleep(0.2)  # Give it a little bit of time to finish
    # Check that the Task is complete now
    assert task_with_check.was_successful()


def test_task_with_check_not_complete_before_run(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Check that the Task is not complete yet
    assert task_with_check.is_complete() is False


def test_task_with_check_not_successful_before_run(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Check that the Task is not complete yet
    assert task_with_check.was_successful() is None


def test_task_with_check_complete_before_running_if_already_complete(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Create the test file before we start to simulate the Task is complete
    (fixed_tmp_dir / TEST_FILE).touch()
    assert (fixed_tmp_dir / TEST_FILE).is_file()
    # At this point, task_with_check should already be complete
    assert task_with_check.is_complete() is True


def test_task_with_check_successful_before_running_if_already_complete(
    fixed_tmp_dir: Path, task_with_check: Task
) -> None:
    # Create the test file before we start to simulate the Task is complete
    (fixed_tmp_dir / TEST_FILE).touch()
    assert (fixed_tmp_dir / TEST_FILE).is_file()
    # At this point, task_with_check should already be complete
    assert task_with_check.was_successful() is True


# Test task_with_sleep


@pytest.mark.slow
@pytest.mark.parametrize(
    "wait_time_before_checking, result",
    [(TEST_SLEEP_TIME / 10, False), (TEST_SLEEP_TIME + 0.5, True)],
)
def test_task_with_sleep_runs(
    fixed_tmp_dir: Path,
    task_with_sleep: Task,
    wait_time_before_checking: float,
    result: bool,
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    task_with_sleep.run()
    # Wait for some amount of time and check if the file exists
    time.sleep(wait_time_before_checking)
    assert (fixed_tmp_dir / TEST_FILE).is_file() is result


@pytest.mark.slow
@pytest.mark.parametrize(
    "wait_time_before_checking, result",
    [(TEST_SLEEP_TIME / 10, False), (TEST_SLEEP_TIME + 0.5, True)],
)
def test_task_with_sleep_is_complete_after_run(
    fixed_tmp_dir: Path,
    task_with_sleep: Task,
    wait_time_before_checking: float,
    result: bool,
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    task_with_sleep.run()
    # Wait for some amount of time and check if the file exists
    time.sleep(wait_time_before_checking)
    assert task_with_sleep.is_complete() is result


@pytest.mark.slow
@pytest.mark.parametrize(
    "wait_time_before_checking, result",
    [(TEST_SLEEP_TIME / 10, None), (TEST_SLEEP_TIME + 0.5, True)],
)
def test_task_with_sleep_was_successful_after_run(
    fixed_tmp_dir: Path,
    task_with_sleep: Task,
    wait_time_before_checking: float,
    result: bool,
) -> None:
    # Make sure that the test file does not exist before we start
    assert (fixed_tmp_dir / TEST_FILE).is_file() is False
    # Run the Task
    task_with_sleep.run()
    # Wait for some amount of time and check if the file exists
    time.sleep(wait_time_before_checking)
    assert task_with_sleep.was_successful() is result


# Test Task equality check


@pytest.mark.parametrize(
    "task1, task2",
    [
        (Task(command="long command"), Task(command="long command")),
        (
            Task(command="long command"),
            Task(command="long command", short_string="short"),
        ),
        (
            Task(command="long command", short_string="short"),
            Task(command="long command"),
        ),
        (
            Task(command="long command", short_string="short"),
            Task(command="long command", short_string="short"),
        ),
        (
            Task(command="long command", short_string="short1"),
            Task(command="long command", short_string="short2"),
        ),
        (
            Task(command="long command"),
            Task(command="long command", priority=1),
        ),
        (
            Task(command="long command", priority=1),
            Task(command="long command"),
        ),
    ],
)
def test_tasks_equal(task1: Task, task2: Task) -> None:
    assert task1 == task2


@pytest.mark.parametrize(
    "task1, task2",
    [
        (Task(command="command1"), Task(command="command2")),
        (
            Task(command="command1"),
            Task(command="command2", short_string="short"),
        ),
        (
            Task(command="command1", short_string="short"),
            Task(command="command2"),
        ),
        (
            Task(command="command1", short_string="short"),
            Task(command="command2", short_string="short"),
        ),
        (
            Task(command="command1", short_string="short1"),
            Task(command="command2", short_string="short2"),
        ),
        (
            Task(command="command1"),
            Task(command="command2", priority=1),
        ),
        (
            Task(command="command1", priority=1),
            Task(command="command2"),
        ),
    ],
)
def test_tasks_not_equal(task1: Task, task2: Task) -> None:
    assert task1 != task2


def test_task_not_equal_to_string(basic_task: Task) -> None:
    with pytest.raises(NotImplementedError):
        basic_task == basic_task.command
