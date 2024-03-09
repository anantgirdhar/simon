import subprocess
from typing import Callable, Optional


class Task:
    def __init__(
        self,
        command: str,
        priority: int = 0,
        short_string: str = "",
        completion_check: Optional[Callable[[], bool]] = None,
    ) -> None:
        super().__init__()
        self._process: Optional[subprocess.Popen] = None  # type: ignore
        self.priority = priority
        self.command = command
        self.short_string = short_string
        if not completion_check:
            # If no additional check is provided, then create a function that
            # always returns False when asked if it is complete. This way, the
            # code will always just default to the internal check (which checks
            # whether or not the subprocess is complete).
            self.completion_check: Callable[[], bool] = lambda: False
        else:
            self.completion_check = completion_check

    def run(self) -> None:
        """Run the task"""
        if self.is_complete():
            return
        command = self.command  # + ' >/dev/null'
        self._process = subprocess.Popen(
            command, shell=True, stdout=subprocess.DEVNULL
        )

    def is_complete(self) -> bool:
        if self.completion_check():
            return True
        if self._process is None:
            return False
        return_value = self._process.poll()
        if return_value is None:
            return False
        return True

    def was_successful(self) -> Optional[bool]:
        if not self.is_complete():
            return None
        if self._process:
            return_value = self._process.poll()
            if return_value == 0:
                return True
            else:
                return False
        # If there is no process instance, but the task is complete, then we
        # were successful
        return True

    def __repr__(self) -> str:
        return f"[Task: {self.command}]"

    def __str__(self) -> str:
        if self.short_string:
            return f"[Task: {self.short_string}]"
        else:
            return f"[Task: {self.command}]"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Task):
            raise NotImplementedError
        return self.command == other.command
