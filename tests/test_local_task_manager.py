from datetime import datetime
from pathlib import Path
from typing import List
from unittest import mock

import pytest
from simon.cluster.local import LocalJobManager
from simon.task import Task

COMPRESS_PID = "513849050313"

# Case directory setup convenience functions


@pytest.fixture
def case_dir(tmp_path: Path) -> Path:
    directory = tmp_path
    return directory


@pytest.fixture
def cluster(case_dir: Path) -> LocalJobManager:
    return LocalJobManager(case_dir=case_dir)


@pytest.fixture
def files_list(case_dir: Path) -> List[str]:
    # Create the files and return the list of them
    files = ["1e-4", "0.001", "0.01", "0.1", "1", "10.0", "100"]
    for f in files:
        # Write out some information to these files so that they're unique
        file_contents = f"File name: {f}\nCreated at: {datetime.now()}"
        with open(case_dir / f, "w") as outfile:
            outfile.write(file_contents)
    return files


@pytest.fixture
def directories_list(case_dir: Path) -> List[str]:
    # Create the directories, with some files and return the list of them
    directories = ["1e-4", "0.001", "0.01", "0.1", "1", "10.0", "100"]
    files = ["file1.txt", "file2.txt", "file3.txt"]
    for d in directories:
        # Create the directory
        (case_dir / d).mkdir()
        for f in files:
            # Write out some information to these files so that they're unique
            file_contents = f"File name: {d}/{f}\nCreated at: {datetime.now()}"
            with open(case_dir / d / f, "w") as outfile:
                outfile.write(file_contents)
    return directories


# Mock Task so that it does not run anything


def print_instead_of_running(task: Task, block: bool = False) -> None:
    print(f"Command: {task.command}", end="")


@pytest.fixture
def mocked_task_run():
    with mock.patch.object(Task, "run", autospec=True) as _mocked_task_run:
        _mocked_task_run.side_effect = print_instead_of_running
        yield _mocked_task_run


@pytest.fixture
def mocked_get_process_status_found():
    with mock.patch.object(
        LocalJobManager, "_get_process_status", autospec=True
    ) as _mocked_get_job_id:
        _mocked_get_job_id.return_value = "JOB_FOUND"
        yield _mocked_get_job_id


@pytest.fixture
def mocked_get_process_status_not_found():
    with mock.patch.object(
        LocalJobManager, "get_job_status", autospec=True
    ) as _mocked_get_job_id:
        _mocked_get_job_id.return_value = "JOB_NOT_FOUND"
        yield _mocked_get_job_id


def assert_task_did_not_run(capfd) -> None:
    out, err = capfd.readouterr()
    assert out == ""


def assert_task_ran(capfd) -> None:
    out, err = capfd.readouterr()
    assert out.startswith("Command: ")


# Test compressing


def test_compress_handles_empty_tgz_file_name(
    cluster: LocalJobManager, files_list: List[str], mocked_task_run, capfd
) -> None:
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="", files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_handles_tgz_file_name_with_spaces(
    cluster: LocalJobManager, files_list: List[str], mocked_task_run, capfd
) -> None:
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="some tgz file.tgz", files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_handles_empty_files_list(
    cluster: LocalJobManager, files_list: List[str], mocked_task_run, capfd
) -> None:
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=[])
    assert_task_did_not_run(capfd)


def test_compress_handles_files_with_spaces_in_name(
    cluster: LocalJobManager, mocked_task_run, capfd
) -> None:
    files_list = ["file1", "file_2", "file 3", "file_4", "file5"]
    # Create these files
    for f in files_list:
        (cluster.case_dir / f).touch()
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_handles_non_existent_files(
    cluster: LocalJobManager, files_list: List[str], mocked_task_run, capfd
) -> None:
    # Remove one of the files
    (cluster.case_dir / files_list[2]).unlink()
    with pytest.raises(FileNotFoundError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_generates_correct_command_for_files(
    cluster: LocalJobManager, files_list: List[str]
) -> None:
    tgz_file = "some_tgz_file.tgz"
    compress_command = cluster._create_compress_command(
        tgz_file=tgz_file, files=files_list
    )
    true_tar_command = "tar -czvf some_tgz_file.tgz.inprogress.$$ 1e-4 0.001 0.01 0.1 1 10.0 100"
    true_compress_command = (
        "mv some_tgz_file.tgz.queued some_tgz_file.tgz.inprogress.$$"
        + " && "
        + true_tar_command
        + " && mv some_tgz_file.tgz.inprogress.$$ some_tgz_file.tgz"
        + " && echo Done compressing some_tgz_file.tgz!"
    )
    assert compress_command == true_compress_command


def test_compress_generates_correct_command_for_directories(
    cluster: LocalJobManager, directories_list: List[str]
) -> None:
    test_tgz = "some_tgz_file.tgz"
    compress_command = cluster._create_compress_command(
        tgz_file=test_tgz, files=directories_list
    )
    true_tar_command = (
        f"tar -czvf {test_tgz}.inprogress.$$ 1e-4 0.001 0.01 0.1 1 10.0 100"
    )
    true_compress_command = (
        f"mv {test_tgz}.queued {test_tgz}.inprogress.$$"
        + " && "
        + true_tar_command
        + f" && mv {test_tgz}.inprogress.$$ {test_tgz}"
        + f" && echo Done compressing {test_tgz}!"
    )
    assert compress_command == true_compress_command


def test_compress_generates_correct_sfile(
    cluster: LocalJobManager, files_list: List[str]
) -> None:
    # Create an arbitrary command that should be added in
    random_command = "this is random && it needs to be added into the sfile"
    # Create the compress script
    with cluster._create_compress_script(
        random_command
    ) as generated_sfile_path:
        # Read the contents of the generated file
        with open(generated_sfile_path, "r") as genfile:
            generated_contents = genfile.read()
    assert generated_contents == random_command


def test_compress_deletes_generated_sfile_after_use(
    cluster: LocalJobManager, files_list: List[str]
) -> None:
    with cluster._create_compress_script(
        "random command"
    ) as generated_sfile_path:
        # The generated sfile should exist here
        assert generated_sfile_path.is_file()
    # And it should be cleaned up by here
    assert not generated_sfile_path.is_file()


def test_compress_does_not_run_when_queued(
    cluster: LocalJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_process_status_found,
    capfd,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    # Simulate that the compression task is queued by creating the queued file
    # placeholder
    queued_file_placeholder = tgz_file + ".queued"
    (cluster.case_dir / queued_file_placeholder).touch()
    cluster.compress(tgz_file=tgz_file, files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_does_not_run_when_inprogress(
    cluster: LocalJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_process_status_found,
    capfd,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    # Simulate that the compression task is in progress by creating the in
    # progress file
    inprogress_file = tgz_file + f".inprogress.{COMPRESS_PID}"
    (cluster.case_dir / inprogress_file).touch()
    cluster.compress(tgz_file=tgz_file, files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_does_not_run_when_completed(
    cluster: LocalJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_process_status_found,
    capfd,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    # Simulate that the compression task is complete by creating the tgz_file
    (cluster.case_dir / tgz_file).touch()
    cluster.compress(tgz_file=tgz_file, files=files_list)
    assert_task_did_not_run(capfd)


def test_compress_does_run_when_not_started(
    cluster: LocalJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_process_status_found,
    capfd,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    cluster.compress(tgz_file=tgz_file, files=files_list)
    assert_task_ran(capfd)
