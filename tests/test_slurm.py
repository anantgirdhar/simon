import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import List
from unittest import mock

import pytest
from simon.cluster.slurm import SlurmJobManager
from simon.task import Task

JOB_SFILE_NAME = "case.sbatch"
COMPRESS_SFILE_NAME = "compress.sbatch.template"
SLURM_JOB_ID = "19810412"
COMPRESS_JOB_ID = "513849050313"

# Case directory setup convenience functions


@pytest.fixture
def case_dir(tmp_path: Path) -> Path:
    directory = tmp_path
    sample_job_sfile = Path(__file__).with_name("sample_case.sbatch")
    sample_compress_sfile = Path(__file__).with_name("sample_compress.sbatch")
    if not sample_job_sfile.is_file():
        raise FileNotFoundError(f"{sample_job_sfile} not found")
    if not sample_compress_sfile.is_file():
        raise FileNotFoundError(f"{sample_compress_sfile} not found")
    destination_job_sfile = directory / JOB_SFILE_NAME
    shutil.copy(sample_job_sfile, destination_job_sfile)
    destination_compress_sfile = directory / COMPRESS_SFILE_NAME
    shutil.copy(sample_compress_sfile, destination_compress_sfile)
    return directory


@pytest.fixture
def cluster(case_dir: Path) -> SlurmJobManager:
    return SlurmJobManager(
        case_dir=case_dir,
        job_sfile=JOB_SFILE_NAME,
        job_id=SLURM_JOB_ID,
        compress_sfile=COMPRESS_SFILE_NAME,
    )


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


# Test initialization


def test_in_valid_case_dir(case_dir: Path) -> None:
    # This should not raise an error
    SlurmJobManager(
        case_dir=case_dir,
        job_sfile=JOB_SFILE_NAME,
        job_id=SLURM_JOB_ID,
        compress_sfile=COMPRESS_SFILE_NAME,
    )


@pytest.mark.parametrize(
    "leave_out_file", [JOB_SFILE_NAME, COMPRESS_SFILE_NAME]
)
def test_in_bad_case_dir(case_dir: Path, leave_out_file: str) -> None:
    # Remove the file to leave out
    (case_dir / leave_out_file).unlink()
    with pytest.raises(ValueError):
        SlurmJobManager(
            case_dir=case_dir,
            job_sfile=JOB_SFILE_NAME,
            job_id=SLURM_JOB_ID,
            compress_sfile=COMPRESS_SFILE_NAME,
        )


def test_can_extract_job_name(cluster: SlurmJobManager) -> None:
    assert cluster.job_name == "sample_job_name"


# Mock Task so that it does not run anything


@pytest.fixture
def mocked_task_run():
    with mock.patch.object(Task, "run", autospec=True) as _mocked_task_run:
        yield _mocked_task_run


@pytest.fixture
def mocked_get_job_name():
    with mock.patch.object(
        SlurmJobManager, "get_job_name", autospec=True
    ) as _mocked_get_job_name:
        _mocked_get_job_name.return_value = "sample_compress_job_name"
        yield _mocked_get_job_name


@pytest.fixture
def mocked_get_job_status_pending():
    with mock.patch.object(
        SlurmJobManager, "get_job_status", autospec=True
    ) as _mocked_get_job_id:
        _mocked_get_job_id.return_value = "PENDING"
        yield _mocked_get_job_id


@pytest.fixture
def mocked_get_job_status_does_not_exist():
    with mock.patch.object(
        SlurmJobManager, "get_job_status", autospec=True
    ) as _mocked_get_job_id:
        _mocked_get_job_id.return_value = "JOB_NOT_FOUND"
        yield _mocked_get_job_id


@pytest.fixture
def mocked_get_dependent_jobs_queued():
    with mock.patch.object(
        SlurmJobManager, "_get_dependent_jobs", autospec=True
    ) as _mocked_get_dependent_jobs:
        _mocked_get_dependent_jobs.return_value = ["123456789"]
        yield _mocked_get_dependent_jobs


@pytest.fixture
def mocked_get_dependent_jobs_none():
    with mock.patch.object(
        SlurmJobManager, "_get_dependent_jobs", autospec=True
    ) as _mocked_get_dependent_jobs:
        _mocked_get_dependent_jobs.return_value = []
        yield _mocked_get_dependent_jobs


# Test requeueing


def test_requeue_generates_correct_command(
    cluster: SlurmJobManager,
    mocked_task_run,
    mocked_get_dependent_jobs_none,
) -> None:
    cluster.requeue_job()
    mocked_task_run.assert_called_once()
    args = mocked_task_run.mock_calls[0].args
    assert len(args) == 1
    assert isinstance(args[0], Task)
    generated_command = args[0].command
    assert mocked_task_run.mock_calls[0].kwargs == {"block": True}
    cwd = os.getcwd()
    true_command = (
        f"cd {cluster.case_dir}"
        f" && sbatch --parsable -d afterany:{SLURM_JOB_ID} {JOB_SFILE_NAME}"
        f" && cd {cwd}"
    )
    assert generated_command == true_command


def test_requeue_does_not_run_twice(
    cluster: SlurmJobManager, mocked_task_run, mocked_get_dependent_jobs_queued
) -> None:
    # With the mocked get dependent jobs, it should not requeue the job
    cluster.requeue_job()
    mocked_task_run.assert_not_called()


def test_requeue_handles_missing_sfile(
    cluster: SlurmJobManager,
    mocked_task_run,
) -> None:
    # Remove the sfile
    (cluster.case_dir / cluster.job_sfile).unlink()
    with pytest.raises(FileNotFoundError):
        cluster.requeue_job()
    mocked_task_run.assert_not_called()


def test_requeue_handles_changed_job_name(
    cluster: SlurmJobManager,
    mocked_task_run,
) -> None:
    # Instead of changing the job name in the sfile, change it in the
    # SlurmJobManager instance so that it no longer matches the sfile to
    # simulate this
    cluster.job_name = "very_random_new_job_name"
    with pytest.raises(AttributeError):
        cluster.requeue_job()
    mocked_task_run.assert_not_called()


# Test compressing


def test_compress_handles_empty_tgz_file_name(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
) -> None:
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="", files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_handles_tgz_file_name_with_spaces(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
) -> None:
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="some tgz file.tgz", files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_handles_empty_files_list(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
) -> None:
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=[])
    mocked_task_run.assert_not_called()


def test_compress_handles_files_with_spaces_in_name(
    cluster: SlurmJobManager,
    mocked_task_run,
) -> None:
    files_list = ["file1", "file_2", "file 3", "file_4", "file5"]
    # Create these files
    for f in files_list:
        (cluster.case_dir / f).touch()
    with pytest.raises(ValueError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_handles_non_existent_files(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
) -> None:
    # Remove one of the files
    (cluster.case_dir / files_list[2]).unlink()
    with pytest.raises(FileNotFoundError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_handles_missing_sfile(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
) -> None:
    # Remove the sfile
    (cluster.case_dir / cluster.compress_sfile).unlink()
    with pytest.raises(FileNotFoundError):
        cluster.compress(tgz_file="some_tgz_file.tgz", files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_generates_correct_command_for_files(
    cluster: SlurmJobManager, files_list: List[str]
) -> None:
    tgz_file = "some_tgz_file.tgz"
    compress_command = cluster._create_compress_command(
        tgz_file=tgz_file, files=files_list
    )
    true_tar_command = "tar -czvf some_tgz_file.tgz.inprogress.$SLURM_JOB_ID 1e-4 0.001 0.01 0.1 1 10.0 100"
    true_compress_command = (
        "mv some_tgz_file.tgz.queued some_tgz_file.tgz.inprogress.$SLURM_JOB_ID"
        + " && "
        + true_tar_command
        + " && mv some_tgz_file.tgz.inprogress.$SLURM_JOB_ID some_tgz_file.tgz"
        + " && echo Done compressing some_tgz_file.tgz!"
    )
    assert compress_command == true_compress_command


def test_compress_generates_correct_command_for_directories(
    cluster: SlurmJobManager, directories_list: List[str]
) -> None:
    test_tgz = "some_tgz_file.tgz"
    compress_command = cluster._create_compress_command(
        tgz_file=test_tgz, files=directories_list
    )
    true_tar_command = f"tar -czvf {test_tgz}.inprogress.$SLURM_JOB_ID 1e-4 0.001 0.01 0.1 1 10.0 100"
    true_compress_command = (
        f"mv {test_tgz}.queued {test_tgz}.inprogress.$SLURM_JOB_ID"
        + " && "
        + true_tar_command
        + f" && mv {test_tgz}.inprogress.$SLURM_JOB_ID {test_tgz}"
        + f" && echo Done compressing {test_tgz}!"
    )
    assert compress_command == true_compress_command


def test_compress_generates_correct_sfile(
    cluster: SlurmJobManager, files_list: List[str]
) -> None:
    # Read the contents of the template sfile in
    with open(cluster.case_dir / COMPRESS_SFILE_NAME, "r") as origfile:
        sfile_contents = origfile.read()
    # Create an arbitrary command that should be added in
    random_command = "this is random && it needs to be added into the sfile"
    # Add it to what the sfile_contents should become
    sfile_contents += f"\n\n{random_command}"
    # Now create the new sfile
    with cluster._create_compress_sfile(
        random_command
    ) as generated_sfile_path:
        # Read the contents of the generated file
        with open(generated_sfile_path, "r") as genfile:
            generated_contents = genfile.read()
    assert generated_contents == sfile_contents


def test_compress_deletes_generated_sfile_after_use(
    cluster: SlurmJobManager, files_list: List[str]
) -> None:
    with cluster._create_compress_sfile(
        "random command"
    ) as generated_sfile_path:
        # The generated sfile should exist here
        assert generated_sfile_path.is_file()
    # And it should be cleaned up by here
    assert not generated_sfile_path.is_file()


def test_compress_does_not_run_when_queued(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_job_status_pending,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    # Simulate that the compression task is queued by creating the queued file
    # placeholder
    queued_file_placeholder = tgz_file + ".queued"
    (cluster.case_dir / queued_file_placeholder).touch()
    cluster.compress(tgz_file=tgz_file, files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_does_not_run_when_inprogress(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_job_status_pending,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    # Simulate that the compression task is in progress by creating the in
    # progress file
    inprogress_file = tgz_file + f".inprogress.{COMPRESS_JOB_ID}"
    (cluster.case_dir / inprogress_file).touch()
    cluster.compress(tgz_file=tgz_file, files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_does_not_run_when_completed(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_job_status_pending,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    # Simulate that the compression task is complete by creating the tgz_file
    (cluster.case_dir / tgz_file).touch()
    cluster.compress(tgz_file=tgz_file, files=files_list)
    mocked_task_run.assert_not_called()


def test_compress_does_run_when_not_started(
    cluster: SlurmJobManager,
    files_list: List[str],
    mocked_task_run,
    mocked_get_job_status_pending,
) -> None:
    tgz_file = "test_tgz_file.tgz"
    cluster.compress(tgz_file=tgz_file, files=files_list)
    mocked_task_run.assert_called_once()
    args = mocked_task_run.mock_calls[0].args
    assert len(args) == 1
    assert isinstance(args[0], Task)
    generated_command = args[0].command
    assert mocked_task_run.mock_calls[0].kwargs == {"block": True}
    true_command = (
        f"touch {cluster.case_dir}/{tgz_file}.queued"
        + f" && sbatch {cluster.case_dir}/{COMPRESS_SFILE_NAME}.filled"
    )
    assert generated_command == true_command
