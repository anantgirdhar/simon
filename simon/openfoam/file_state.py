import decimal
from decimal import Decimal
from pathlib import Path
from typing import List, Tuple

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
            ],
            key=float,
        )

    def is_reconstructed(self, timestamp: str) -> bool:
        reconstruction_done_marker_filepath = (
            self.case_dir / timestamp / RECONSTRUCTION_DONE_MARKER_FILENAME
        )
        return reconstruction_done_marker_filepath.is_file()

    def is_tarred(self, timestamp: str) -> bool:
        if (self.case_dir / f"{timestamp}.tar").is_file():
            return True
        else:
            return False

    def is_compressed(self, timestamp: str) -> bool:
        # Check if this timestamp is in any of the compressed files based on
        # the filename
        t = Decimal(timestamp)
        for compressed_file in self.case_dir.glob("times_*.tgz"):
            start_time, end_time, step = self.extract_compressed_file_params(
                compressed_file.name
            )
            if t < start_time:
                continue
            if t > end_time:
                continue
            if t % step == 0:
                return True
        return False

    def is_compressed_file(self, filename: str) -> bool:
        if not filename.endswith(".tgz"):
            return False
        if not filename.startswith("times_"):
            return False
        try:
            self.extract_compressed_file_params(filename)
        except decimal.InvalidOperation:
            return False
        if (self.case_dir / filename).is_file():
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

    @staticmethod
    def create_compressed_filename(start: str, end: str, step: str) -> str:
        try:
            Decimal(start)
        except decimal.InvalidOperation:
            raise ValueError(
                f"start {start} must be a string that can be turned into a float"
            )
        try:
            Decimal(end)
        except decimal.InvalidOperation:
            raise ValueError(
                f"end {end} must be a string that can be turned into a float"
            )
        try:
            Decimal(step)
        except decimal.InvalidOperation:
            raise ValueError(
                f"step {step} must be a string that can be turned into a float"
            )
        return f"times_{start}_{end}_{step}.tgz"

    @staticmethod
    def extract_compressed_file_params(
        filename: str,
    ) -> Tuple[Decimal, Decimal, Decimal]:
        # Remove the file extension
        filename = Path(filename).stem
        # Everything should now be separated by underscores
        _, start, end, step = filename.split("_")
        return (Decimal(start), Decimal(end), Decimal(step))
