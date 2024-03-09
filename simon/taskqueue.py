import itertools as it

from simon.priority_list import PriorityList
from simon.task import Task


class TaskQueue:
    def __init__(self, num_simultaneous_tasks: int = 6) -> None:
        self.num_simultaneous_tasks = num_simultaneous_tasks
        self._running: list[Task] = []
        self._queue: PriorityList[Task] = PriorityList()

    def add(self, *tasks: Task) -> None:
        for task in tasks:
            self._queue.add(task, task.priority)
        self.update()

    def update(self) -> None:
        # Remove any complete tasks from the running list
        for task in self._running:
            if task.is_complete():
                self._running.remove(task)
                # TODO: Consider if we need to do something here depending on
                # task success / failure
        # Ensure that the running list is filled back up with pending tasks
        while len(self._running) < self.num_simultaneous_tasks and self._queue:
            next_pending_task = self._queue.pop()
            self._running.append(next_pending_task)
            self._running[-1].run()

    def __len__(self) -> int:
        return len(self._running) + len(self._queue)

    def __iter__(self):  # type: ignore
        return it.chain(self._running, self._queue)

    def __repr__(self) -> str:
        return (
            f"[Task Queue: {len(self._running)} running / {len(self)} tasks]"
        )

    def __str__(self) -> str:
        task_list_string = f"{len(self)} tasks in queue:\n"
        for task in self._running:
            task_list_string += "  * " + str(task) + "\n"
        for task in self._queue:
            task_list_string += "  - " + str(task) + "\n"
        return task_list_string
