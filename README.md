## HDFS.jl

HDFS.jl provides a wrapper over libhdfs and a julia map-reduce functionality built on top of HDFS. The file system APIs provided can be used for direct access to HDFS files. A parallel map-reduce is also provided, based on these APIs.

- [HDFS APIs](HDFS.md) : HDFS file system APIs
- [HDFS Map Reduce Interface](MAPREDUCE.md) : Map - Reduce APIs
- [HDFS Blocks Interface](BLOCKS.md) : HDFS Blocks APIs
- [Installation](INSTALL.md) : Installation instructions and FAQ

Note: HDFS.jl is based on libhdfs.jl, originally written by Benjamin Yang (@benyang) and part of the julia/extras.
