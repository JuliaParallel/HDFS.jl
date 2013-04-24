HDFS.jl
=======

HDFS.jl provides a wrapper over libhdfs and a julia map-reduce functionality built on top of HDFS. The file libhdfs.jl was originally written by Benjamin Yang (@benyang) and part of the julia/extras. Methods in HDFS.jl can be used for low level functionality, using direct HDFS APIs.

File hdfs\_jobs.jl provides a simple parallel map-reduce functionality. Since this is work in progress, it may not be stable enough at all times. The following are required in order to use it.


Map Reduce Setup:
-----------------
- Julia must be setup at identical location on all data nodes.
- Authorized keys setup for ssh from name node machine to all data nodes
- A job file must be written that defines:

````
1. find_rec(jc::JobContext)
    Identifies what consists a record.
    Creates the record from input buffer, stores record and pos in buff to read the next record from in the job context.
    Find should also implement the filter that ignores (and skips over) records that are not interesting for the current job.
    find_rec should suitably read beyond the block to read complete records when required.
2. process_rec(jc::JobContext)
    Process the record and accumulate results
3. reduce(jc::JobContext, results::Vector)
    Provided with all results to do the reduce step.
4. summarize(results)
    Provided with results from reduce steps for the final summarization.
````
- The job file must be placed at identical location on all data nodes. (in future a way to do this automatically would be provided)


Map Reduce Execution:
---------------------
- **require**("job\_file.jl")<br/>Imports job definition.
- **hdfs\_do_job**("hdfshost", hdfsport, "file_to_process", record, results)<br/>This would run the job on all available julia instances (presumably on all data nodes), schedule processing of data blocks on nodes that preferably have the data block locally.


Test:
-----
A few sample test scrips are provided in the test folder. The test scripts currently work on curated twitter data as provided from [infochimps](http://www.infochimps.com/datasets/twitter-census-conversation-metrics-one-year-of-urls-hashtags-sm--2)

- twitter\_test\_single\_node.jl: Uses the base HDFS APIs to work with files, but in a non distributed manner.
- twitter\_test.jl: Uses the map-reduce framework described above to execute the same job on multiple worker nodes.


TODO:
-----
- Handle records spanning across block boundaries
- Better scheduling of blocks across processing nodes
- Distributed reduction step
- Scalability improvements, e.g. multiple tasks per node, rack awareness
- Ability to work on multiple small files, instead of one large file spanning blocks
- Resilience to node failure
- Multi step map-reduce in memory
- API usability (convenience julia macros/methods to make working with HDFS intuitive)
- Integrate other distributed storage systems: MapR, Cassandra, MongoDB etc


