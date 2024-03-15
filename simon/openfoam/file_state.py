from pathlib import Path
from typing import List

RECONSTRUCTION_DONE_MARKER_FILENAME = ".__reconstruction_done"


class OFFileState:
    def __init__(
        self,
        case_dir: Path,
    ) -> None:
        if not self._is_valid_openfoam_dir(case_dir):
            raise ValueError(
                "This does not appear to be a valid OpenFOAM root case dir."
            )
        self.case_dir = case_dir

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

    def get_split_times(self) -> List[str]:
        processor0_directory = self.case_dir / "processor0"
        return sorted(
            [
                t.name
                for t in processor0_directory.glob("[0-9]*")
                if t.is_dir()
            ],
            key=float,
        )

    def get_reconstructed_times(self) -> List[str]:
        return sorted(
            [
                t.name
                for t in self.case_dir.glob("[0-9]*")
                if t.is_dir()
                and (t / RECONSTRUCTION_DONE_MARKER_FILENAME).is_file()
            ],
            key=float,
        )

    def get_tarred_times(self) -> List[str]:
        return sorted(
            [
                t.stem  # Remove the .tar file extension to get just the time
                for t in self.case_dir.glob("[0-9]*.tar")
            ]
        )

    def is_reconstructed(self, timestamp: str) -> bool:
        reconstruction_done_marker_filepath = (
            self.case_dir / timestamp / RECONSTRUCTION_DONE_MARKER_FILENAME
        )
        return (
            self.is_tarred(timestamp)
            or reconstruction_done_marker_filepath.is_file()
        )

    def is_tarred(self, timestamp: str) -> bool:
        if (self.case_dir / f"{timestamp}.tar").is_file():
            return True
        else:
            return False

    def reconstructed_dir_exists(self, timestamp: str) -> bool:
        # This checks that the directory exists
        # It does not make any guarantees that it is fully written
        if (self.case_dir / f"{timestamp}").is_dir():
            return True
        return False

    def split_exists(self, timestamp: str) -> bool:
        # This checks that the split timestamp directory exists in at least one
        # of the processor directories. It does not make any guarantees that it
        # is fully written
        for processor_directory in self.case_dir.glob("processor*"):
            if (processor_directory / timestamp).is_dir():
                return True
        return False
