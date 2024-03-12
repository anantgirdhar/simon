from decimal import Decimal
from pathlib import Path
from typing import List

from simon.task import Task

RECONSTRUCTION_DONE_MARKER_FILENAME = ".__reconstruction_done"


class OFListener:
    def __init__(
        self, keep_every: Decimal, case_dir: Path = Path(".")
    ) -> None:
        if not self._is_valid_openfoam_dir(case_dir):
            raise ValueError(
                "This does not appear to be a valid OpenFOAM root case dir."
            )
        self.case_dir = case_dir
        self.keep_every = keep_every
        self._processed_split_times: List[str] = []
        self._processed_reconstructed_times: List[str] = []
        self._processed_tarred_times: List[str] = []

    @staticmethod
    def _is_valid_openfoam_dir(case_dir: Path) -> bool:
        # To check that we're in an OpenFOAM case dir, check to see if we have:
        # - a constant dir
        # - a system dir
        # - a processor0 dir
        if not (case_dir / "constant").is_dir():
            return False
        if not (case_dir / "system").is_dir():
            return False
        if not (case_dir / "processor0").is_dir():
            return False
        return True

    def get_new_tasks(self) -> List[Task]:
        new_tasks: List[Task] = []
        new_split_times = self._get_new_split_times()
        for i, t in enumerate(new_split_times):
            if i == len(new_split_times) - 1:
                # Remove the last split time from consideration. This is
                # because it is possible that OpenFOAM is still writing out the
                # last split time files, in which case, it's not ready for
                # further processing (reconstruction) yet.
                continue
            if self._delete_without_processing(Decimal(t)):
                new_tasks.append(self._create_delete_split_task(t))
                self._processed_split_times.append(t)
            elif self._was_successfully_reconstructed(t):
                new_tasks.append(self._create_delete_split_task(t))
                self._processed_split_times.append(t)
            else:
                new_tasks.append(self._create_reconstruct_task(t))
                self._processed_split_times.append(t)
        for t in self._get_new_reconstructed_times():
            # TODO: Before deleting this split time, make sure that the
            # following split time already exists
            new_tasks.append(self._create_delete_split_task(t))
            if not self._was_successfully_tarred(t):
                new_tasks.append(self._create_tar_task(t))
            self._processed_reconstructed_times.append(t)
        for t in self._get_new_tarred_times():
            new_tasks.append(self._create_delete_reconstructed_task(t))
            self._processed_tarred_times.append(t)
        return new_tasks

    def _delete_without_processing(self, timestep: Decimal) -> bool:
        # The timestep should be deleted if it is not a "multiple" of
        # self.keep_every
        # timestep is a Decimal to deal with floating point weirdnesses
        # The timestamp is a "multiple" of self.keep_every if the timestamp
        # does not have a fractional part when divided by self.keep_every
        quotient = timestep / self.keep_every
        return quotient % 1 != 0

    def _get_new_split_times(self) -> List[str]:
        processor0_directory = self.case_dir / "processor0"
        return sorted(
            [
                t.name
                for t in processor0_directory.glob("[0-9]*")
                if t.is_dir() and t.name not in self._processed_split_times
            ],
            key=float,
        )

    def _get_new_reconstructed_times(self) -> List[str]:
        return sorted(
            [
                t.name
                for t in self.case_dir.glob("[0-9]*")
                if t.is_dir()
                and (t / RECONSTRUCTION_DONE_MARKER_FILENAME).is_file()
                and t.name not in self._processed_reconstructed_times
            ],
            key=float,
        )

    def _get_new_tarred_times(self) -> List[str]:
        return sorted(
            [
                t.stem  # Remove the .tar file extension to get just the time
                for t in self.case_dir.glob("[0-9]*.tar")
                if t.stem not in self._processed_tarred_times
            ]
        )

    def _create_reconstruct_task(self, timestamp: str) -> Task:
        reconstruct_command = f"reconstructPar -time {timestamp}"
        if self.case_dir != Path("."):
            reconstruct_command += f" -case {self.case_dir}"
        reconstruction_done_marker_filepath = (
            Path(self.case_dir)
            / timestamp
            / RECONSTRUCTION_DONE_MARKER_FILENAME
        )
        post_reconstruct_command = (
            f"touch {reconstruction_done_marker_filepath}"
        )
        command = " && ".join([reconstruct_command, post_reconstruct_command])
        return Task(
            command=command,
            priority=2,
            short_string=f"Reconstruct {timestamp}",
        )

    def _create_delete_split_task(self, timestamp: str) -> Task:
        return Task(
            command=f"rm -rf {self.case_dir}/processor*/{timestamp}",
            priority=0,
            short_string=f"DeleteSplit {timestamp}",
        )

    def _create_delete_reconstructed_task(self, timestamp: str) -> Task:
        return Task(
            command=f"rm -rf {self.case_dir}/{timestamp}",
            priority=0,
            short_string=f"DeleteReconstructed {timestamp}",
        )

    def _create_tar_task(self, timestamp: str) -> Task:
        reconstruction_done_marker_filepath = (
            Path(self.case_dir)
            / timestamp
            / RECONSTRUCTION_DONE_MARKER_FILENAME
        )
        tar_in_progress_path = f"{self.case_dir}/{timestamp}.tar.inprogress"
        tar_path = f"{self.case_dir}/{timestamp}.tar"
        timestamp_path = f"{self.case_dir}/{timestamp}"
        tar_command = (
            f"tar --exclude {reconstruction_done_marker_filepath} "
            + f"-cvf {tar_in_progress_path} {timestamp_path}"
        )
        post_tar_command = f"mv {tar_in_progress_path} {tar_path}"
        command = " && ".join([tar_command, post_tar_command])
        return Task(
            command=command, priority=1, short_string=f"Tar {timestamp}"
        )

    def _was_successfully_reconstructed(self, timestamp: str) -> bool:
        reconstruction_done_marker_filepath = (
            self.case_dir / timestamp / RECONSTRUCTION_DONE_MARKER_FILENAME
        )
        return (
            self._was_successfully_tarred(timestamp)
            or reconstruction_done_marker_filepath.is_file()
        )

    def _was_successfully_tarred(self, timestamp: str) -> bool:
        if (self.case_dir / f"{timestamp}.tar").is_file():
            return True
        else:
            return False

    def _reconstructed_dir_exists(self, timestamp: str) -> bool:
        # This checks that the directory exists
        # It does not make any guarantees that it is fully written
        if (self.case_dir / f"{timestamp}").is_dir():
            return True
        return False

    def _split_exists(self, timestamp: str) -> bool:
        # This checks that the split timestamp directory exists in at least one
        # of the processor directories. It does not make any guarantees that it
        # is fully written
        for processor_directory in self.case_dir.glob("processor*"):
            if (processor_directory / timestamp).is_dir():
                return True
        return False
