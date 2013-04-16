HDFS.jl
=======

Provides a wrapper over libhdfs and a julia map-reduce functionality built on top of HDFS.

Methods in HDFS.jl can be used for low level functionality, using direct HDFS APIs.

File hdfs\_jobs.jl provides a simple parallel map-reduce functionality. The following are required in order to use it.

Setup:
------
- Julia must be setup at identical location on all data nodes.
- Authorized keys setup for ssh from name node machine to all data nodes
- A job file must be written that defines:

````
1. find_record(buff::Array{Uint8,1}, start_pos::Int64, len::Int64)
    Identifies what consists a record.
    Creates the record from input buffer, returns record, pos in buff to read the next record from.
    Find should also implement the filter that ignores (and skips over) records that are not interesting for the current job.
2. process_record(rec)
    Process the record and accumulate results
3. gather_results()
    Collects results accumulated till now. This is called from the central node to collect data for the reduction step.
    Called after processing of a block (distributed processing)
    Called after processing of complete file (local processing)
4. init_job_ctx()
    Called before data processing methods are called to create globals, and job contexts
5. get_job_ctx()
    Gets the current job context.
6. destroy_job_ctx()
    Called after the job is completed. This should cleanup stuff created in init_job_ctx
````
- The job file must be placed at identical location on all data nodes. (in future a way to do this automatically would be provided)


Execution:
----------
- **require**("job\_file.jl")<br/>Imports job definition.
- **hdfs\_job\_do\_parallel**(["datanode1", "datanode2",…], "job\_file.jl")<br/>This would bring up remote julia instances on all data nodes, schedule processing of data blocks on nodes that preferably have the data block locally.


Test:
-----
A few sample test scrips are provided in the test folder. The test scripts currently work on curated twitter data as provided from [infochimps](http://www.infochimps.com/datasets/twitter-census-conversation-metrics-one-year-of-urls-hashtags-sm--2)

- twitter\_test\_single\_node.jl: Uses the base HDFS APIs to work with files, but in a non distributed manner.
- twitter\_test.jl: Uses the map-reduce framework described above to execute the same job on multiple worker nodes.


TODO:
-----
- Distributed reduction step
- Scalability improvements, e.g. multiple tasks per node, rack awareness
- Ability to work on multiple small files, instead of one large file spanning blocks
- Resilience to node failure
- Multi step map-reduce in memory
- API usability (convenience julia macros/methods to make working with HDFS intuitive)
- Integrate other distributed storage systems: MapR, Cassandra, MongoDB etc

