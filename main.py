#!/usr/bin/python3

import sys
import time
from pathlib import Path

from simon.oflistener import OFListener
from simon.taskqueue import TaskQueue


def main(
    keep_every: str,
    num_simultaneous_tasks: int = 6,
    sleep_time_per_update: int = 2,
    recheck_every_num_updates: int = 1,
) -> None:
    num_updates = 0
    listener = OFListener(keep_every, Path("."))
    task_queue = TaskQueue(num_simultaneous_tasks=num_simultaneous_tasks)
    # initiate_incomplete_tasks_from_last_run(task_queue)
    task_queue.add(*listener.get_new_tasks())
    while len(task_queue) > 0 or recheck_every_num_updates > 0:
        num_updates += 1
        if len(task_queue) > 0:
            print("\nUpdating task queue")
            print(task_queue)
            task_queue.update()
        time.sleep(sleep_time_per_update)
        # if recheck_every_num_updates <= 0 and len(task_queue) == 0:
        #     return
        if (
            recheck_every_num_updates > 0
            and num_updates % recheck_every_num_updates == 0
        ):
            task_queue.add(*listener.get_new_tasks())


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print(
            f"Usage: {sys.argv[0]}"
            " keep_every"
            " num_simultaneous_tasks"
            " sleep_time_per_update"
            " [recheck_every_num_updates]"
        )
        sys.exit(-1)
    keep_every = sys.argv[1]
    num_simultaneous_tasks = int(sys.argv[2])
    sleep_time_per_update = int(sys.argv[3])
    recheck_every_num_updates = int(sys.argv[4]) if len(sys.argv) > 4 else 1
    main(
        keep_every,
        num_simultaneous_tasks,
        sleep_time_per_update,
        recheck_every_num_updates,
    )
