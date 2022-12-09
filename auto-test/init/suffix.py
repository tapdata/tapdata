#!/usr/bin/env python
import os
import random
import time

table_suffix_cache_file = os.path.dirname(os.path.abspath(__file__)) + "/.table_suffix_cache_file"


def get_test_table(datasource, table):
    return "%s%s.%s%s" % (datasource, get_suffix(), table, get_suffix())


# make a random suffix, to support multi thread case running
def get_suffix():
    if os.path.exists(table_suffix_cache_file):
        # suffix is same in one time case running
        with open(table_suffix_cache_file, "r") as fd:
            table_suffix = fd.readline().rstrip()
            return table_suffix
    table_suffix = "_%d_%d" % (int(time.time() * 1000), random.randint(1, 10000))
    with open(table_suffix_cache_file, "w+") as fd:
        fd.write(table_suffix)
    return table_suffix


if __name__ == "__main__":
    get_suffix()
