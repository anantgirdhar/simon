import bisect
import itertools as it
from typing import Generic, Iterator, TypeVar

T = TypeVar("T")


class PriorityList(Generic[T]):
    """A bare-bones implementation of a Priority List

    This assumes that the priorities are integers and the items are sorted in
    order of increasing priority number (similar to a min heap). If two items
    have the same priority, the one added first will occur earlier in the
    array.
    """

    def __init__(self) -> None:
        # Create a dictionary that stores lists of items for each priority
        self.__items: dict[int, list[T]] = {}
        # Also create an ordered list of all the priorities for easier access
        self.__priorities: list[int] = []

    def add(self, item: T, priority: int = 0) -> None:
        if priority < 0:
            raise ValueError("Priority should be non-negative, not {priority}")
        if priority not in self.__items:
            self.__items[priority] = []
            bisect.insort(self.__priorities, priority)
        self.__items[priority].append(item)

    def pop(self) -> T:
        if len(self) == 0:
            raise IndexError("pop from empty list")
        # Get the highest priority
        priority = self.__priorities[0]
        # Get the item from that priority
        item = self.__items[priority].pop(0)
        # If this priority level is empty, remove it
        if not self.__items[priority]:
            self.__items.pop(priority)
            self.__priorities.pop(0)
        return item

    def __iter__(self) -> Iterator[T]:
        return it.chain(
            *(self.__items[priority] for priority in self.__priorities)
        )

    def __len__(self) -> int:
        return sum(len(list_) for list_ in self.__items.values())

    def __str__(self) -> str:
        # Build the list of items
        items = []
        for priority in self.__priorities:
            items.extend(self.__items[priority])
        return str(items)

    def __repr__(self) -> str:
        return repr(self.__items)
