import os
import re
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List

from simon.task import Task

JOB_NAME_REGEX = re.compile(r"#SBATCH\s+-J\s+([a-zA-Z0-9_-]+)$")


class SlurmJobManager:
    def __init__(
        self, case_dir: Path, job_sfile: str, job_id: str, compress_sfile: str
    ) -> None:
        self.case_dir = case_dir
        self.job_sfile = job_sfile
        self.job_id = job_id
        self.compress_sfile = compress_sfile
        self._verify_directory_is_valid()
        self.job_name = self._read_job_name()

    def _verify_directory_is_valid(self) -> None:
        if not (self.case_dir / self.job_sfile).is_file():
            raise ValueError(
                f"{self.case_dir} does not contain {self.job_sfile}"
            )
        if not (self.case_dir / self.compress_sfile).is_file():
            raise ValueError(
                f"{self.case_dir} does not contain {self.job_sfile}"
            )
        # TODO: Ensure that the job ID is correct

    def _read_job_name(self) -> str:
        with open(self.case_dir / self.job_sfile) as f:
            # TODO: Is this correct??
            for line in f:
                if m := re.search(JOB_NAME_REGEX, line):
                    return m.group(1)
        raise Exception("Unable to find job name.")

    def get_job_status(self, job_id: str) -> str:
        squeue_output = (
            subprocess.check_output(
                f"squeue --me --jobs={job_id} -o '%i %j %T'", shell=True
            )
            .decode("utf-8")
            .split("\n")
        )
        # Remove any empty lines
        while "" in squeue_output:
            squeue_output.remove("")
        # If there is only the one header line, then this job does not exist
        if len(squeue_output) == 1:
            return "JOB_NOT_FOUND"
        if len(squeue_output) < 1 or len(squeue_output) > 2:
            raise ValueError(
                f"Got something weird back from squeue:\n{squeue_output}"
            )
        # Otherwise the status is the third element
        return squeue_output[1].split(" ")[2]

    def get_job_name(self, job_id: str) -> str:
        squeue_output = (
            subprocess.check_output(
                f"squeue --me --jobs={job_id} -o '%i %j %T'", shell=True
            )
            .decode("utf-8")
            .split("\n")
        )
        # Remove any empty lines
        while "" in squeue_output:
            squeue_output.remove("")
        # If there is only the one header line, then this job does not exist
        if len(squeue_output) == 1:
            return "JOB_NOT_FOUND"
        if len(squeue_output) < 1 or len(squeue_output) > 2:
            raise ValueError(
                f"Got something weird back from squeue:\n{squeue_output}"
            )
        # Otherwise the job name is the second element
        return squeue_output[1].split(" ")[1]

    def requeue_job(self) -> None:
        if not (self.case_dir / self.job_sfile).is_file():
            raise FileNotFoundError(f"sfile {self.job_sfile} does not exist")
        if (new_job_name := self._read_job_name()) != self.job_name:
            raise AttributeError(
                f"The job name in the sfile {new_job_name} does not match"
                f" what this was started with {self.job_name}"
            )
        cwd = os.getcwd()
        commands = [
            f"cd {self.case_dir}",
            f"sbatch --parsable -d afterany:{self.job_id} {self.job_sfile}",
            f"cd {cwd}",
        ]
        # TODO: Make sure that this gets higher priority than anyone else
        Task(command=" && ".join(commands), priority=0).run(block=True)

    def _create_compress_command(self, tgz_file: str, files: List[str]) -> str:
        tar_command = f"tar -czvf {tgz_file}.inprogress.$SLURM_JOB_ID"
        for f in files:
            tar_command += f" {f}"
        commands = [
            f"mv {tgz_file}.queued {tgz_file}.inprogress.$SLURM_JOB_ID",
            tar_command,
            f"mv {tgz_file}.inprogress.$SLURM_JOB_ID {tgz_file}",
            f"echo Done compressing {tgz_file}!",
        ]
        return " && ".join(commands)

    @contextmanager
    def _create_compress_sfile(self, compress_command: str) -> Iterator[Path]:
        # Create a copy of the compress sfile template
        filled_compress_sfile = f"{self.compress_sfile}.filled"
        filled_compress_sfile_path = self.case_dir / filled_compress_sfile
        shutil.copy(
            self.case_dir / self.compress_sfile,
            filled_compress_sfile_path,
        )
        # Add the compress_command to it
        with open(filled_compress_sfile_path, "a") as outfile:
            outfile.write(f"\n\n{compress_command}")
        # Yield the name of the file
        yield filled_compress_sfile_path
        # Now clean up the file
        filled_compress_sfile_path.unlink()

    def compress(self, tgz_file: str, files: List[str]) -> None:
        # Compress some files to out_file then delete the files that were
        # compressed
        # This function needs to be aware of the state of this job possibly
        # across multiple runs, i.e., it needs to somehow be awaare that it the
        # compression is in progress even if it is restarted
        # Here is my attempt at keeping track of that:
        # - The tgz_file name should be unique
        # - Create a blank file in the output directory as a placeholder for
        #   the job being queued
        # - Then create a compression job that replaces this blank file with an
        #   "inprogress" compressed file that has the SLURM JOB_ID appended to
        #   it
        # - Before attempting to start the compression, check if the "queued"
        #   file or the "inprogress" file exist
        # - If the "inprogress" file exists, check if the job is still running
        # - If not, go ahead and submit a new compression job
        # This method allows to monitor the state of the compression without
        # having to maintain an extra state file. It also provides some
        # robustness against changing job names for the main task since the
        # state of the job is tied to the tgz_file requested (which hopefully
        # will be unique).
        # First make sure the inputs are good
        if not tgz_file:
            raise ValueError("No output tgz_file specified")
        if " " in tgz_file:
            raise ValueError(f"No spaces allowed in file names ({tgz_file})")
        if not files:
            raise ValueError("No files to compress")
        for f in files:
            if " " in f:
                raise ValueError(f"No spaces allowed in file names ({f})")
            if not (self.case_dir / f).is_file():
                raise FileNotFoundError(f"File {f} not found")
        if not (self.case_dir / self.compress_sfile).is_file():
            raise FileNotFoundError(
                f"sfile {self.compress_sfile} does not exist"
            )
        tgz_path = self.case_dir / tgz_file
        # Next make sure that we haven't already run it
        queued_file = tgz_path.with_name(tgz_file + ".queued")
        if queued_file.is_file():
            return
        inprogress_glob = self.case_dir.glob(f"{tgz_file}.inprogress.*")
        for match in inprogress_glob:
            inprogress_file = match.name
            inprogress_job_id = inprogress_file.split(".")[-1]
            status = self.get_job_status(inprogress_job_id)
            if status != "JOB_NOT_FOUND":
                # The job exists in slurm
                return
        if tgz_path.is_file():
            # The tar file already exists
            # Nothing to do here
            return
        # inprogress_glob = self.case_dir.glob(f"{tgz_file}.inprogress.*")
        # # Check if any of the state files exist for this tar
        # Finally, we can run the compress command
        with self._create_compress_sfile(
            self._create_compress_command(tgz_file, files)
        ) as filled_compress_sfile:
            commands = [
                f"touch {tgz_path}.queued",
                f"sbatch {filled_compress_sfile}",
            ]
            Task(command=" && ".join(commands), priority=0).run(block=True)
        # TODO: As a possible improvement, rename the queued file so that it
        # contains the job ID
