from __future__ import print_function

from ddtrace import tracer
import os

if __name__ == '__main__':
    assert tracer.tags["env"] == "test"
    print("Test success")