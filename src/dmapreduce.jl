module MapReduce
import  Base.close, Base.eof, Base.read, Base.write, Base.readbytes, Base.peek,
        Base.readall, Base.flush, Base.nb_available, Base.position, Base.filesize, Base.seek, Base.seekend, Base.seekstart, Base.skip

import  Base.start, Base.done, Base.next, 
        Base.wait

export  
        # IO methods
        close, eof, read, write, readbytes, peek, readall, flush, nb_available, position, filesize, seek, seekend, seekstart, skip,
        # from hdfs_jobs.jl
        dmap, dmapreduce, results, status, unload, wait, times, JobId, start_workers,
        # from hdfs_reader.jl
        MRInput, MRMapInput, MRFileInput,
        BlockIO,
        HdfsBlockReader, MapResultReader

using HDFS
using URLParse
using PTools
using DataFrames

global _debug = false
function _set_debug(d)
    global _debug
    _debug = d
end

abstract MapInputReader 
abstract MRInput

typealias JobId     Int64
typealias FuncNone  Union(Function,Nothing)

include("blockio.jl")
include("hdfs_reader.jl")
include("map_result_reader.jl")
include("hdfs_jobs.jl")
include("hdfs_mrutils.jl")

end

