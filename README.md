# MapReduce

A distributed MapReduce system for the MIT 6.824 lab. A coordinator assigns work to parallel workers over RPC and recovers tasks when workers fail.

Some key features that I implemented:
- Map execution and partitioned intermediate files
- Reduce execution and sorted key grouping
- Task scheduling, phase coordination, and worker shutdown
- Task reassignment after ten-second timeouts
- Atomic file writes and protection against stale worker output
