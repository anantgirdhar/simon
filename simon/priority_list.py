from typing import Generic, Iterator, TypeVar

T = TypeVar("T")


class PriorityList(Generic[T]):
    """A bare-bones implementation of a Priority List

    This assumes that the priorities are integers and the items are sorted in
    order of increasing priority number (similar to a min heap). If two items
    have the same priority, the one added first will occur earlier in the
    array.
    """

    def __init__(self, num_priorities: int) -> None:
        if num_priorities <= 0:
            raise ValueError(
                "The number of priorities should be positive,"
                + f" not {num_priorities}."
            )
        self.__list: list[T] = []
        # Create a list where each element gives the next index to insert an
        # element with the same priority as the index of that element
        self.__priority_indices = [
            0,
        ] * num_priorities

    def add(self, item: T, priority: int = 0) -> None:
        if priority < 0:
            raise ValueError("Priority should be non-negative, not {priority}")
        try:
            index = self.__priority_indices[priority]
        except IndexError:
            raise ValueError(
                f"Priority should be less than {len(self.__priority_indices)} not {priority}"
            )
        self.__list.insert(index, item)
        for index in range(priority, len(self.__priority_indices)):
            self.__priority_indices[index] += 1

    def pop(self) -> T:
        if len(self) == 0:
            raise IndexError("pop from empty list")
        item = self.__list.pop(0)
        self.__priority_indices = [
            max(index - 1, 0) for index in self.__priority_indices
        ]
        return item

    def __iter__(self) -> Iterator[T]:
        return iter(self.__list)

    def __len__(self) -> int:
        return len(self.__list)

    def __str__(self) -> str:
        return str(self.__list)

    def __repr__(self) -> str:
        return repr(self.__list)
