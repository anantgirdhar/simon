#!/usr/bin/python3

import argparse
import time
from decimal import Decimal
from pathlib import Path

from simon.oflistener import OFListener
from simon.taskqueue import TaskQueue


def init_argparse() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="simon", description="A Simulation Monitor"
    )
    parser.add_argument(
        "command",
        choices=["setup", "monitor"],
        help="Running mode 'setup' (clean up case directory) or 'monitor'",
    )
    parser.add_argument(
        "--keep-every",
        required=True,
        dest="keep_every",
        type=Decimal,
        help="How often to keep timesteps (all others are deleted)",
    )
    parser.add_argument(
        "-n",
        "--num-simultaneous-tasks",
        default=4,
        dest="num_simultaneous_tasks",
        type=int,
        help="How many tasks to run in parallel",
    )
    parser.add_argument(
        "-s",
        "--sleep-time-per-update",
        default=2,
        dest="sleep_time_per_update",
        type=int,
        help="How many seconds to sleep for between update steps",
    )
    parser.add_argument(
        "-u",
        "--recheck-every-num-updates",
        default=1,
        dest="recheck_every_num_updates",
        type=int,
        help="How many update steps to run before querying for new tasks",
    )
    return parser


def setup(
    keep_every: Decimal,
    num_simultaneous_tasks: int,
    sleep_time_per_update: int,
    case_directory: Path = Path("."),
) -> None:
    listener = OFListener(keep_every, case_directory)
    task_queue = TaskQueue(num_simultaneous_tasks=num_simultaneous_tasks)
    task_queue.add(*listener.get_cleanup_tasks())
    # Run all the tasks in the task queue to completion before proceeding
    while True:
        print("\nUpdating task queue")
        task_queue.update()
        print(task_queue)
        if len(task_queue) == 0:
            break
        time.sleep(sleep_time_per_update)
    listener.ensure_case_correctness()


def monitor(
    keep_every: Decimal,
    num_simultaneous_tasks: int,
    sleep_time_per_update: int,
    recheck_every_num_updates: int,
    case_directory: Path = Path("."),
) -> None:
    num_updates = 0
    listener = OFListener(keep_every, case_directory)
    task_queue = TaskQueue(num_simultaneous_tasks=num_simultaneous_tasks)
    task_queue.add(*listener.get_new_tasks())
    while len(task_queue) > 0 or recheck_every_num_updates > 0:
        num_updates += 1
        if len(task_queue) > 0:
            print("\nUpdating task queue")
            task_queue.update()
            print(task_queue)
        time.sleep(sleep_time_per_update)
        # if recheck_every_num_updates <= 0 and len(task_queue) == 0:
        #     return
        if (
            recheck_every_num_updates > 0
            and num_updates % recheck_every_num_updates == 0
        ):
            task_queue.add(*listener.get_new_tasks())


def main() -> None:
    parser = init_argparse()
    args = parser.parse_args()
    if args.command == "setup":
        setup(
            keep_every=args.keep_every,
            num_simultaneous_tasks=args.num_simultaneous_tasks,
            sleep_time_per_update=args.sleep_time_per_update,
        )
    elif args.command == "monitor":
        monitor(
            keep_every=args.keep_every,
            num_simultaneous_tasks=args.num_simultaneous_tasks,
            sleep_time_per_update=args.sleep_time_per_update,
            recheck_every_num_updates=args.recheck_every_num_updates,
        )


if __name__ == "__main__":
    main()
