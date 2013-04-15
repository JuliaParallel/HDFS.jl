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
1. find\_record(buff::Array{Uint8,1}, start\_pos::Int64, len::Int64)
  Identifies what consists a record
    Creates the record from input buffer, returns record, pos in buff to read the next record from.
2. process\_record(rec)
    process the record and accumulate results
3. gather\_results()
    collects results accumulated till now
    called after processing of a block (distributed processing)
    called after processing of complete file (local processing)
4. init\_job\_ctx()
    called before data processing methods are called to create globals, and job contexts
5. get\_job\_ctx()
    gets the current job context.
6. destroy\_job\_ctx()
    called after the job is completed. This should cleanup stuff created in init_job_ctx
````
- The job file must be placed at identical location on all data nodes. (in future a way to do this automatically would be provided)


Execution:
----------
- require("job\_file.jl")
- hdfs\_job\_do\_parallel(["datanode1", "datanode2",…], "job\_file.jl")

This would bring up remote julia instances on all data nodes, schedule processing of data blocks on nodes that preferably have the data block locally.


TODO:
-----
- Scalability improvements, e.g. multiple tasks per node, rack awareness
- Ability to work on multiple small files, instead of one large file spanning blocks
- Resilience to node failure
- Multi step map-reduce in memory
- API usability (convenience julia macros/methods to make working with HDFS intuitive)
- Integrate other distributed storage systems: MapR, Cassandra, MongoDB etc

