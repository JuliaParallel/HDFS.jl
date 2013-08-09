## The HDFS Blocks Interface
HDFS being a distributed data store integrates well with the [Blocks Framework](https://github.com/tanmaykm/Blocks.jl) for easy distributed data processing.

````
using Blocks
using HDFS

Block(file::HdfsURL)
    Each chunk is a block in HDFS.
    Processor affinity of each chunk is set to machines where this block has been replicated by HDFS.
````

The test script [blocks_test.jl](test/blocks_test.jl) serves as a sample to illustrate how map-reduce can be done using HDFS blocks.


