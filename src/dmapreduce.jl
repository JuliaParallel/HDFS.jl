module MapReduce

import  Base.close, Base.eof, Base.read, Base.write, Base.readbytes, Base.peek,
        Base.readall, Base.flush, Base.nb_available, Base.position, Base.filesize, Base.seek, Base.seekend, Base.seekstart, Base.skip

import  Base.start, Base.done, Base.next, Base.wait

export  close, eof, read, write, readbytes, peek, readall, flush, nb_available, position, filesize, seek, seekend, seekstart, skip,
        # from hdfs_jobs.jl
        dmap, dmapreduce, results, status, unload, wait, times, JobId, start_workers,
        # from hdfs_reader.jl
        MRInput, BlockIO,
        MRMapInput, MRHdfsFileInput, MRFsFileInput,
        MapResultReader, HdfsBlockReader, FsBlockReader

using HDFS
using URLParse
using PTools
#using DataFrames

global _debug = false
function _set_debug(d)
    global _debug
    _debug = d
end

function is_loaded(x::Symbol)
    try
        return (Module == typeof(eval(Main,x)))
    catch
        return false
    end
end

abstract MapInputReader 
abstract MapStreamInputReader <: MapInputReader
abstract MRInput

close(r::MapStreamInputReader) = try close(get_stream(r)) end

typealias JobId     Int64
typealias FuncNone  Union(Function,Nothing)

include("blockio.jl")
include("fs_reader.jl")
include("hdfs_reader.jl")
include("map_result_reader.jl")
include("hdfs_jobs.jl")
include("hdfs_mrutils.jl")

if(is_loaded(:DataFrames))
    include("hdfs_mrutils_dataframes.jl")
end
end

