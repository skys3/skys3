"""The SkyS3 performance harness (plan M7-06a, design section 16.3).

`perf.py` beside this package is its command line; docs/skys3-performance.md
tells how to run it. The package uses only the Python standard library, as
the end-to-end suite and the nightly chaos suite do.
"""

SCHEMA_NAME = "skys3-perf-result"
SCHEMA_VERSION = 1
