import math
from decimal import Decimal
from pathlib import Path
from typing import List, Protocol

from simon.openfoam.file_state import (RECONSTRUCTION_DONE_MARKER_FILENAME,
                                       OFFileState)
from simon.task import Task


class ExternalJobManager(Protocol):
    def requeue_job(self) -> None:
        ...

    def compress(self, tgz_file: str, files: List[str]) -> None:
        ...


class OFListener:
    def __init__(
        self,
        *,
        state: OFFileState,
        keep_every: Decimal,
        compress_every: Decimal,
        cluster: ExternalJobManager,
        requeue: bool = True,
    ) -> None:
        self.state = state
        self.__verify_compress_every_and_keep_every_are_valid(
            compress_every=compress_every, keep_every=keep_every
        )
        self._keep_every = keep_every
        self._compress_every = compress_every
        self.cluster = cluster
        self.requeue = requeue
        self._requeued = False
        self._processed_split_times: List[str] = []
        self._processed_reconstructed_times: List[str] = []
        self._deleted_reconstructed_times: List[str] = []
        self._requested_compressed_files: List[str] = []
        self._deleted_tarred_times: List[str] = []

    def get_new_tasks(self) -> List[Task]:
        new_tasks: List[Task] = []
        # Get the current state of the files
        split_times = self.state.get_split_times()
        reconstructed_times = self.state.get_reconstructed_times()
        tarred_times = self.state.get_tarred_times()
        compressed_files = self.state.get_compressed_files()
        # Generate the new tasks based on the current state
        new_tasks.extend(self._process_split_times(split_times))
        new_tasks.extend(
            self._process_reconstructed_times(reconstructed_times, split_times)
        )
        new_tasks.extend(self._process_tarred_times(tarred_times))
        new_tasks.extend(
            self._process_compressed_files(compressed_files, tarred_times)
        )
        return new_tasks

    def _process_split_times(self, split_times: List[str]) -> List[Task]:
        new_tasks: List[Task] = []
        # Remove the last split time from consideration. This is because it is
        # possible that OpenFOAM is still writing out the last split time
        # files, in which case, it's not ready for further processing
        # (reconstruction) yet.
        for t in split_times[:-1]:
            if t in self._processed_split_times:
                continue
            if self._delete_without_processing(Decimal(t)):
                new_tasks.append(self._create_delete_split_task(t))
                self._processed_split_times.append(t)
            elif self.state.is_reconstructed(t) or self.state.is_tarred(t):
                new_tasks.append(self._create_delete_split_task(t))
                self._processed_split_times.append(t)
            else:
                new_tasks.append(self._create_reconstruct_task(t))
                self._processed_split_times.append(t)
                if self.requeue and not self._requeued:
                    self.cluster.requeue_job()
        return new_tasks

    def _process_reconstructed_times(
        self, reconstructed_times: List[str], split_times: List[str]
    ) -> List[Task]:
        new_tasks: List[Task] = []
        for t in reconstructed_times:
            if t in self._processed_reconstructed_times:
                continue
            if not self.state.is_tarred(t):
                new_tasks.append(self._create_tar_task(t))
            # Delete its split time if it is not the last split time
            if split_times and t != split_times[-1]:
                new_tasks.append(self._create_delete_split_task(t))
                # Mark the time as completed if the split time is deleted
                # because the tar task has already been dealt with
                self._processed_reconstructed_times.append(t)
        return new_tasks

    def _process_tarred_times(self, tarred_times: List[str]) -> List[Task]:
        new_tasks: List[Task] = []
        for t in tarred_times:
            if t in self._deleted_reconstructed_times:
                continue
            new_tasks.append(self._create_delete_reconstructed_task(t))
            self._deleted_reconstructed_times.append(t)
        self._compress_tars(tarred_times)
        return new_tasks

    def _compress_tars(self, tarred_times: List[str]) -> None:
        # We can find how many timestamps are in each compression candidate
        # because compress_every is a multiple of keep_every
        num_tars_to_compress = int(self.compress_every / self.keep_every)
        # If we don't have enough tars, then we can just quit here
        if len(tarred_times) < num_tars_to_compress:
            return
        # Idea:
        # let ce = how often we need to compress files (compress_every)
        # let t0 = the first tarred time available
        # let tN = the last tarred time available
        t0 = Decimal(tarred_times[0])
        tN = Decimal(tarred_times[-1])
        # Let i be an integer > 1
        # Then we can express the start times as: ts_i = i * ce
        # We'll define the end times as: te_i = ts_i + ce
        # This should work similar to python range objects which do not include
        # the end value in the list of items
        # Now we can find the list of start times that are currently available
        # To do this, we can find the values of i corresponding to the first
        # available start time and last available start time
        i_start = math.floor(t0 / self.compress_every)
        i_end = math.ceil(tN / self.compress_every)
        start_times = iter(
            [i * self.compress_every for i in range(i_start, i_end + 1)]
        )
        # Now we can loop over the available times to see if we have the tars
        # needed for each compression candidate
        # And all the times are sorted so that should make things a lot easier
        ts = next(start_times)
        # Next we can find the corresponding end time
        te = ts + self.compress_every
        # Create a list that will store the current compression candidate
        compression_candidate: List[str] = []
        for t in tarred_times:
            if Decimal(t) < ts:
                continue
            if Decimal(t) >= te:
                # We've started the next compression candidate
                ts = next(start_times)
                te = ts + self.compress_every
                compression_candidate = []
            compression_candidate.append(t)
            if len(compression_candidate) == num_tars_to_compress:
                # We've found a complete compression candidate
                tgz_filename = self.state.create_compressed_filename(
                    start=compression_candidate[0],
                    end=compression_candidate[-1],
                    step=str(self.keep_every),
                )
                if tgz_filename in self._requested_compressed_files:
                    continue
                if self.state.is_compressed_file(tgz_filename):
                    continue
                self.cluster.compress(tgz_filename, compression_candidate)
                self._requested_compressed_files.append(tgz_filename)

    def _process_compressed_files(
        self, compressed_files: List[str], tarred_times: List[str]
    ) -> List[Task]:
        new_tasks: List[Task] = []
        for t in tarred_times:
            if not self.state.is_compressed(t):
                continue
            if t in self._deleted_tarred_times:
                continue
            new_tasks.append(self._create_delete_tar_task(t))
        return new_tasks

    def get_cleanup_tasks(self) -> List[Task]:
        # Run this function to remove any incomplete items
        # Only run this during the case setup phase
        new_tasks: List[Task] = []
        # Start with removing any incomplete split times
        # To check if a split time is incomplete, run reconstructPar on it
        # This seems like a pretty decent indicator of whether or not it is
        # complete
        # There are some known issues though - if a variable (usually a
        # species) is missing from all of the split time directories,
        # reconstructPar still runs. This may not be desirable but hopefully
        # this is a rare occurance and so this solution should be good enough
        # Start with the most recent split time and work backwards from there
        for t in reversed(self.state.get_split_times()):
            task = self._create_reconstruct_task(t)
            task.run(block=True)
            if not task.was_successful():
                # If it couldn't be reconstructed, delete it
                new_tasks.append(self._create_delete_split_task(t))
                # The partially reconstructed directory will get dealt with in
                # the next segment
                # Now try the next available split time
                continue
        # Now remove any incompletely reconstructed times
        for t in self.state.get_reconstructed_times():
            if not self.state.is_reconstructed(t):
                new_tasks.append(self._create_delete_reconstructed_task(t))
        # We could remove any in progress tars, but these should theoreticaly
        # get dealt with when the time is tarred again (and are easier to spot)
        return new_tasks

    def ensure_case_correctness(self) -> None:
        # Run this function to get the case setup to a point that it can be
        # restarted from
        # Only run this during the case setup phase
        # First make sure the user is running this on a cleaned directory
        if self.get_cleanup_tasks():
            raise Exception("This is not a cleaned directory.")
        # Remove the processor directories if there are no split times
        # These should get recreated when the job is created
        if not self.state.get_split_times():
            print("No split times left. Deleting all processor directories...")
            Task(command=f"rm -rf {self.state.case_dir}/processor*").run(
                block=True
            )
        else:
            print("Found valid split times! Ready to proceed!")
            # We're done because this is a cleaned directory meaning that any
            # split times are valid because we've tried to reconstruct them
            return
        # Further, make sure that there is a reconstructed time that can be
        # decomposed to recreate the processor directories
        reconstructed_times = self.state.get_reconstructed_times()
        if reconstructed_times:
            print("Found some reconstructed times! Ready to proceed!")
            # We're done because this is a cleaned directory meaning that any
            # reconstructed times are complete and can be decomposed
            return
        # Otherwise, look for the last tarred time and try to decompose that
        tarred_times = self.state.get_tarred_times()
        if tarred_times:
            # Decompose the most recent tarred time and then we're done
            newest_tar_time = tarred_times[-1]
            print(f"Untarring {newest_tar_time}...")
            tar_path = f"{self.state.case_dir}/{newest_tar_time}.tar"
            untar_command = (
                f"tar -xvf {tar_path} --directory={self.state.case_dir}"
            )
            reconstruction_done_marker_filepath = (
                Path(self.state.case_dir)
                / newest_tar_time
                / RECONSTRUCTION_DONE_MARKER_FILENAME
            )
            post_untar_command = f"touch {reconstruction_done_marker_filepath}"
            command = " && ".join([untar_command, post_untar_command])
            task = Task(command=command)
            task.run(block=True)
            print("Restored a reconstructed time! Ready to proceed!")
            return
        # If we managed to get here, then there is no suitable decomposition
        # candidate so we can't proceed
        raise Exception("Could not restore case directory to a good state.")

    def _delete_without_processing(self, timestep: Decimal) -> bool:
        # The timestep should be deleted if it is not a "multiple" of
        # self.keep_every
        # timestep is a Decimal to deal with floating point weirdnesses
        # The timestamp is a "multiple" of self.keep_every if the timestamp
        # does not have a fractional part when divided by self.keep_every
        quotient = timestep / self.keep_every
        return quotient % 1 != 0

    def _create_reconstruct_task(self, timestamp: str) -> Task:
        reconstruct_command = f"reconstructPar -time {timestamp}"
        if self.state.case_dir != Path("."):
            reconstruct_command += f" -case {self.state.case_dir}"
        if timestamp == "0":
            reconstruct_command += " -withZero"
        reconstruction_done_marker_filepath = (
            Path(self.state.case_dir)
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
            command=f"rm -rf {self.state.case_dir}/processor*/{timestamp}",
            priority=0,
            short_string=f"DeleteSplit {timestamp}",
        )

    def _create_delete_reconstructed_task(self, timestamp: str) -> Task:
        return Task(
            command=f"rm -rf {self.state.case_dir}/{timestamp}",
            priority=0,
            short_string=f"DeleteReconstructed {timestamp}",
        )

    def _create_tar_task(self, timestamp: str) -> Task:
        reconstruction_done_marker_filepath = (
            Path(self.state.case_dir)
            / timestamp
            / RECONSTRUCTION_DONE_MARKER_FILENAME
        )
        tar_in_progress_path = (
            f"{self.state.case_dir}/{timestamp}.tar.inprogress"
        )
        tar_path = f"{self.state.case_dir}/{timestamp}.tar"
        timestamp_path = f"{self.state.case_dir}/{timestamp}"
        tar_command = (
            f"tar --exclude {reconstruction_done_marker_filepath} "
            + f"-cvf {tar_in_progress_path} {timestamp_path}"
        )
        post_tar_command = f"mv {tar_in_progress_path} {tar_path}"
        command = " && ".join([tar_command, post_tar_command])
        return Task(
            command=command, priority=1, short_string=f"Tar {timestamp}"
        )

    def _create_delete_tar_task(self, timestamp: str) -> Task:
        return Task(
            command=f"rm {self.state.case_dir}/{timestamp}.tar",
            priority=4,
            short_string=f"DeleteTar {timestamp}",
        )

    @staticmethod
    def __verify_compress_every_and_keep_every_are_valid(
        *, compress_every: Decimal, keep_every: Decimal
    ) -> bool:
        if compress_every % keep_every != 0 or compress_every == keep_every:
            raise ValueError(
                f"Compress every ({compress_every})"
                f" should be a multiple of"
                f" keep every ({keep_every})"
            )
        return True

    @property
    def compress_every(self) -> Decimal:
        return self._compress_every

    @compress_every.setter
    def compress_every(self, value: Decimal) -> None:
        self.__verify_compress_every_and_keep_every_are_valid(
            compress_every=value, keep_every=self.keep_every
        )
        self._compress_every = value

    @property
    def keep_every(self) -> Decimal:
        return self._keep_every

    @keep_every.setter
    def keep_every(self, value: Decimal) -> None:
        self.__verify_compress_every_and_keep_every_are_valid(
            compress_every=self.compress_every, keep_every=value
        )
        self._keep_every = value

    def update_processing_frequencies(
        self, *, keep_every: Decimal, compress_every: Decimal
    ) -> None:
        self.__verify_compress_every_and_keep_every_are_valid(
            compress_every=compress_every, keep_every=keep_every
        )
        self._keep_every = keep_every
        self._compress_every = compress_every
