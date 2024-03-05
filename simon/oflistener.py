from decimal import Decimal
from pathlib import Path
from typing import List

import simon.tasks as tasks

RECONSTRUCTION_DONE_MARKER_FILENAME = ".__reconstruction_done"


class OFListener:
    def __init__(self, keep_every: str, case_dir: Path = Path(".")) -> None:
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

    def get_new_tasks(self) -> List[tasks.Task]:
        new_tasks: List[tasks.Task] = []
        for t in self._get_new_split_times():
            if self._delete_without_processing(t):
                new_tasks.append(tasks.DeleteSplitTask(t))
                self._processed_split_times.append(t)
            else:
                new_tasks.append(tasks.ReconstructTask(t))
                self._processed_split_times.append(t)
        for t in self._get_new_reconstructed_times():
            # TODO: Before deleting this split time, make sure that the
            # following split time already exists
            new_tasks.append(tasks.DeleteSplitTask(t))
            new_tasks.append(tasks.TarTask(t))
            self._processed_reconstructed_times.append(t)
        for t in self._get_new_tarred_times():
            new_tasks.append(tasks.DeleteReconstructedTask(t))
            self._processed_tarred_times.append(t)
        return new_tasks

    def _delete_without_processing(self, t: str) -> bool:
        # The timestep t should be deleted if it is not a "multiple" of
        # self.keep_every
        # First convert it to a Decimal so we don't have to deal with floating
        # point weirdnesses
        timestamp = Decimal(t)
        # The timestamp is a "multiple" of self.keep_every if the timestamp
        # times (1/self.keep_every) does not have a fractional part
        # But we also have to make sure to account for floating point
        # weirdnesses with (1/self.keep_every)
        factor = Decimal(int(round(1 / float(self.keep_every), 1)))
        if (timestamp * factor) % 1 > 0:
            return True
        else:
            return False

    def _get_new_split_times(self) -> List[str]:
        processor0_directory = self.case_dir / "processor0"
        return sorted(
            [
                t.name
                for t in processor0_directory.glob("[0-9]*")
                if t.is_dir() and t.name not in self._processed_split_times
            ],
            key=float,
        )[:-1]
        # We've removed the last split time from consideration. This is because
        # it is possible that OpenFOAM is still writing out the last split time
        # files, in which case, it's not ready for further processing
        # (reconstruction) yet.

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


# def reconstructed_timestamp_successfully_deleted(timestamp: str) -> bool:
#     if Path(f"{timestamp}").is_dir():
#         return False
#     return True


# def split_timestamp_successfully_deleted(timestamp: str) -> bool:
#     # The split timestamp is successfully deleted if it does not exist in any
#     # of the processor directories
#     for processor_directory in Path(".").glob("processor*"):
#         if (processor_directory / timestamp).is_dir():
#             return False
#     return True


# def timestamp_successfully_reconstructed(timestamp: str) -> bool:
#     reconstruction_done_marker_filepath = (
#         Path(timestamp) / RECONSTRUCTION_DONE_MARKER_FILENAME
#     )
#     return (
#         tarring_successfully_completed(timestamp)
#         or reconstruction_done_marker_filepath.is_file()
#     )


# def tarring_successfully_completed(timestamp: str) -> bool:
#     if Path(f"{timestamp}.tar").is_file():
#         return True
#     else:
#         return False
