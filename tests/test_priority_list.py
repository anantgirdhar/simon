import random

import pytest
from simon.taskqueue import PriorityList

# Test initialization


@pytest.mark.parametrize("num_priorities", [0, -1, -2, -3, -4, -5, -6])
def test_invalid_num_priorities(num_priorities: int) -> None:
    with pytest.raises(ValueError):
        plist: PriorityList[str] = PriorityList(num_priorities)


# Test adding invalid things


@pytest.mark.parametrize("num_priorities", range(1, 10))
@pytest.mark.parametrize("priority", [-1, -2, -3, -4, -5])
def test_add_invalid_priority_negative(
    num_priorities: int, priority: int
) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    with pytest.raises(ValueError):
        plist.add("a", priority)


@pytest.mark.parametrize("num_priorities", range(1, 10))
@pytest.mark.parametrize("priority_offset", [0, 1, 2, 5, 10])
def test_add_invalid_priority_large(
    num_priorities: int, priority_offset: int
) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    with pytest.raises(ValueError):
        plist.add("a", num_priorities + priority_offset)


# Test adding and popping items


@pytest.mark.parametrize("num_priorities", range(1, 10))
def test_pop_empty(num_priorities: int) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    with pytest.raises(IndexError):
        plist.pop()


@pytest.mark.parametrize("num_priorities", range(1, 10))
def test_add_pop_singles(num_priorities: int) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    items = [chr(ord("a") + i) for i in range(10)]
    priorities = [min(i, num_priorities - 1) for i in range(10)]
    for i, (item, priority) in enumerate(zip(items, priorities)):
        plist.add(item, priority)
        assert len(plist) == 1
        popped_item = plist.pop()
        assert popped_item == item
        assert len(plist) == 0


@pytest.mark.parametrize("num_priorities", range(1, 10))
def test_add_pop_multiple(num_priorities: int) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    items = [chr(ord("a") + i) for i in range(10)]
    priorities = [min(i, num_priorities - 1) for i in range(10)]
    for i, (item, priority) in enumerate(zip(items, priorities)):
        plist.add(item, priority)
        assert len(plist) == i + 1
    i = len(plist)
    while len(plist) > 0:
        plist.pop()
        i -= 1
        assert len(plist) == i


# Test len function


@pytest.mark.parametrize("num_priorities", range(1, 10))
def test_len_empty(num_priorities: int) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    assert len(plist) == 0


# Test maintaining order


# Run this test multiple times to try different permutations of the items
# Hopefully this idea is good enough
@pytest.mark.parametrize("execution_number", range(25))
@pytest.mark.parametrize("num_priorities", range(1, 10))
def test_add_pop_order(num_priorities: int, execution_number: int) -> None:
    plist: PriorityList[str] = PriorityList(num_priorities)
    items = [chr(ord("a") + i) for i in range(10)]
    priorities = [min(i, num_priorities - 1) for i in range(10)]
    # Create a random ordering of the items
    # Make sure that the priorities are also shuffled the same way
    items_and_priorities = list(zip(items, priorities))
    random.shuffle(items_and_priorities)
    for i, (item, priority) in enumerate(items_and_priorities):
        plist.add(item, priority)
        # Extract teh subset already added
        items_and_priorities_added = items_and_priorities[: i + 1]
        items_added, priorities_added = zip(
            *sorted(items_and_priorities_added, key=lambda x: x[1])
        )
        assert list(items_added) == list(plist)
