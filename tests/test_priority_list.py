import random
from typing import List

import pytest
from simon.priority_list import PriorityList

# Test adding invalid things


@pytest.mark.parametrize("priority", [-1, -2, -3, -4, -5])
def test_add_invalid_priority_negative(priority: int) -> None:
    plist: PriorityList[str] = PriorityList()
    with pytest.raises(ValueError):
        plist.add("a", priority)


# Test len method


def test_len_empty() -> None:
    plist: PriorityList[str] = PriorityList()
    assert len(plist) == 0


@pytest.mark.parametrize(
    "priorities",
    [
        [0 for _ in range(10)],
        [1 for _ in range(10)],
        [10 for _ in range(10)],
        [i - (i % 2) for i in range(10)],
        list(range(10)),
        list(reversed(range(10))),
        [50 * i for i in range(10)],
        random.sample(range(1000000), 10),
    ],
)
def test_len_add_pop_singles(priorities: List[int]) -> None:
    plist: PriorityList[str] = PriorityList()
    items = [chr(ord("a") + i) for i in range(10)]
    for i, (item, priority) in enumerate(zip(items, priorities)):
        plist.add(item, priority)
        assert len(plist) == 1
        plist.pop()
        assert len(plist) == 0


@pytest.mark.parametrize(
    "priorities",
    [
        [0 for _ in range(10)],
        [1 for _ in range(10)],
        [10 for _ in range(10)],
        [i - (i % 2) for i in range(10)],
        list(range(10)),
        list(reversed(range(10))),
        [50 * i for i in range(10)],
        random.sample(range(1000000), 10),
    ],
)
def test_len_add_pop_multiple(priorities: List[int]) -> None:
    plist: PriorityList[str] = PriorityList()
    items = [chr(ord("a") + i) for i in range(10)]
    for i, (item, priority) in enumerate(zip(items, priorities)):
        plist.add(item, priority)
        assert len(plist) == i + 1
    i = len(plist)
    while len(plist) > 0:
        plist.pop()
        i -= 1
        assert len(plist) == i


# Test maintaining order on add / pop


def test_pop_empty() -> None:
    plist: PriorityList[str] = PriorityList()
    with pytest.raises(IndexError):
        plist.pop()


@pytest.mark.parametrize(
    "priorities",
    [
        [0 for _ in range(10)],
        [1 for _ in range(10)],
        [10 for _ in range(10)],
        [i - (i % 2) for i in range(10)],
        list(range(10)),
        list(reversed(range(10))),
        [50 * i for i in range(10)],
        random.sample(range(1000000), 10),
    ],
)
def test_add_pop_singles_gives_back_same_element(
    priorities: List[int],
) -> None:
    plist: PriorityList[str] = PriorityList()
    items = [chr(ord("a") + i) for i in range(10)]
    for i, (item, priority) in enumerate(zip(items, priorities)):
        plist.add(item, priority)
        assert len(plist) == 1
        popped_item = plist.pop()
        assert popped_item == item
        assert len(plist) == 0


# Run this test multiple times to try different permutations of the items
# Hopefully this idea is good enough
@pytest.mark.parametrize("execution_number", range(25))
@pytest.mark.parametrize(
    "priorities",
    [
        [0 for _ in range(10)],
        [1 for _ in range(10)],
        [10 for _ in range(10)],
        [i - (i % 2) for i in range(10)],
        list(range(10)),
        list(reversed(range(10))),
        [50 * i for i in range(10)],
        random.sample(range(1000000), 10),
    ],
)
def test_add_pop_random_order_works(
    execution_number: int, priorities: List[int]
) -> None:
    plist: PriorityList[str] = PriorityList()
    items = [chr(ord("a") + i) for i in range(10)]
    # Create a random ordering of the items
    # Make sure that the priorities are also shuffled the same way
    items_and_priorities = list(zip(items, priorities))
    random.shuffle(items_and_priorities)
    for i, (item, priority) in enumerate(items_and_priorities):
        plist.add(item, priority)
        # Extract the subset already added
        items_and_priorities_added = items_and_priorities[: i + 1]
        items_added, priorities_added = zip(
            *sorted(items_and_priorities_added, key=lambda x: x[1])
        )
        assert list(items_added) == list(plist)
