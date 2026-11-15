from src.strategies.full_refresh import FullRefresh
from src.strategies.incremental_merge import IncrementalMerge
from src.strategies.partition_replace import PartitionReplace

REGISTRY = {s.name: s for s in (FullRefresh, IncrementalMerge, PartitionReplace)}


def get_strategy(name):
    return REGISTRY[name]()
