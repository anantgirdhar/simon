import os
import re
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, List

from simon.task import Task

JOB_NAME_REGEX = re.compile(r"#SBATCH\s+-J\s+([a-zA-Z0-9_-]+)$")


class LocalJobManager:
    def __init__(self, case_dir: Path) -> None:
        self.case_dir = case_dir

    def _get_process_status(self, pid: str) -> str:
        command_output = (
            subprocess.check_output(
                f"ps --pid {pid} | grep ^{pid}", shell=True
            )
            .decode("utf-8")
            .split("\n")
        )
        # Remove any empty lines
        while "" in command_output:
            command_output.remove("")
        # Figure out what the status is
        if len(command_output) < 1:
            return "JOB_NOT_FOUND"
        elif len(command_output) == 1:
            # TODO: Can we get the status of the process
            return "JOB_FOUND"
        else:
            raise ValueError(
                f"Got something weird back from ps:\n{command_output}"
            )

    def requeue_job(self) -> None:
        # On the local system, we don't have to worry about requeueing the job
        # So don't do anything
        pass

    def _create_compress_command(self, tgz_file: str, files: List[str]) -> str:
        # $$ gets the PID
        tar_command = f"tar -czvf {tgz_file}.inprogress.$$"
        for f in files:
            tar_command += f" {f}"
        commands = [
            f"mv {tgz_file}.queued {tgz_file}.inprogress.$$",
            tar_command,
            f"mv {tgz_file}.inprogress.$$ {tgz_file}",
            f"echo Done compressing {tgz_file}!",
        ]
        return " && ".join(commands)

    @contextmanager
    def _create_compress_script(self, compress_command: str) -> Iterator[Path]:
        temp_compress_script = self.case_dir / "TEMPORARY__COMPRESS__SCRIPT.sh"
        # Add the compress_command to it
        with open(temp_compress_script, "w") as outfile:
            outfile.write(f"{compress_command}")
        # Yield the name of the file
        yield temp_compress_script
        # Now clean up the file
        temp_compress_script.unlink()

    def __verify_compress_inputs(
        self, tgz_file: str, files: List[str]
    ) -> bool:
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
        return True

    def __compress_is_running(self, tgz_file: str) -> bool:
        tgz_path = self.case_dir / tgz_file
        queued_file = tgz_path.with_name(tgz_file + ".queued")
        if queued_file.is_file():
            return True
        inprogress_glob = self.case_dir.glob(f"{tgz_file}.inprogress.*")
        for match in inprogress_glob:
            inprogress_file = match.name
            inprogress_pid = inprogress_file.split(".")[-1]
            status = self._get_process_status(inprogress_pid)
            if status != "JOB_NOT_FOUND":
                # The process exists
                return True
        return False

    def compress(self, tgz_file: str, files: List[str]) -> None:
        # Compress some files to out_file then delete the files that were
        # compressed
        # This function needs to be aware of the state of this process possibly
        # across multiple runs, i.e., it needs to somehow be awaare that the
        # compression is in progress even if it is restarted
        # Here is my attempt at keeping track of that:
        # - The tgz_file name should be unique
        # - Create a blank file in the output directory as a placeholder for
        #   the task being queued
        # - Then create a compression task that replaces this blank file with
        #   an "inprogress" compressed file that has the PID appended to it
        # - Before attempting to start the compression, check if the "queued"
        #   file or the "inprogress" file exist
        # - If the "inprogress" file exists, check if the process is still
        #   running
        # - If not, go ahead and submit a new compression task
        # This method allows to monitor the state of the compression without
        # having to maintain an extra state file
        self.__verify_compress_inputs(tgz_file, files)
        tgz_path = self.case_dir / tgz_file
        if tgz_path.is_file():
            # The tar file already exists
            # Nothing to do here
            return
        if self.__compress_is_running(tgz_file):
            return
        with self._create_compress_script(
            self._create_compress_command(tgz_file, files)
        ) as filled_compress_script:
            # This needs to be run from the case directory because all the
            # filenames are specified relative to the case directory
            cwd = os.getcwd()
            command = (
                f"cd {self.case_dir}"
                f" && touch {tgz_path}.queued"
                f" && sh {filled_compress_script} &"
                f"\ncd {cwd}"
            )
            Task(command=command, priority=0).run(block=True)
