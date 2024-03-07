from decimal import Decimal

# Create test cases that only have split directories
_OFLISTENER_TEST_CASES_SPLIT_ONLY = [
    {
        "keep_every": Decimal("0.1"),
        "split": {
            "0.01": "delete",
            "0.03": "delete",
            "0.05": "delete",
            "0.1": "reconstruct",
            "0.3": "reconstruct",
            "0.5": "reconstruct",
            "1": "reconstruct",
            "3": "reconstruct",
            "5": "reconstruct",
            "10": "reconstruct",
        },
    },
    {
        "keep_every": Decimal("0.3"),
        "split": {
            "0.01": "delete",
            "0.03": "delete",
            "0.05": "delete",
            "0.1": "delete",
            "0.3": "reconstruct",
            "0.5": "delete",
            "1": "delete",
            "3": "reconstruct",
            "5": "delete",
            "10": "delete",
        },
    },
    {
        "keep_every": Decimal("0.5"),
        "split": {
            "0.01": "delete",
            "0.03": "delete",
            "0.05": "delete",
            "0.1": "delete",
            "0.3": "delete",
            "0.5": "reconstruct",
            "1": "reconstruct",
            "3": "reconstruct",
            "5": "reconstruct",
            "10": "reconstruct",
        },
    },
    {
        "keep_every": Decimal("1"),
        "split": {
            "0.01": "delete",
            "0.03": "delete",
            "0.05": "delete",
            "0.1": "delete",
            "0.3": "delete",
            "0.5": "delete",
            "1": "reconstruct",
            "3": "reconstruct",
            "5": "reconstruct",
            "10": "reconstruct",
        },
    },
    {
        "keep_every": Decimal("3"),
        "split": {
            "0.01": "delete",
            "0.03": "delete",
            "0.05": "delete",
            "0.1": "delete",
            "0.3": "delete",
            "0.5": "delete",
            "1": "delete",
            "3": "reconstruct",
            "5": "delete",
            "10": "delete",
        },
    },
    {
        "keep_every": Decimal("5"),
        "split": {
            "0.01": "delete",
            "0.03": "delete",
            "0.05": "delete",
            "0.1": "delete",
            "0.3": "delete",
            "0.5": "delete",
            "1": "delete",
            "3": "delete",
            "5": "reconstruct",
            "10": "reconstruct",
        },
    },
]

# Convert this to a datastructure that can be used with parametrize
OFLISTENER_TEST_CASES_SPLIT_ONLY = []
for test_case in _OFLISTENER_TEST_CASES_SPLIT_ONLY:
    keep_every = test_case["keep_every"]
    split_times_and_actions = list(test_case["split"].items())
    split_times_and_actions.sort(key=lambda x: float(x[0]))
    split_times, actions = zip(*split_times_and_actions)
    OFLISTENER_TEST_CASES_SPLIT_ONLY.append((keep_every, split_times, actions))
