##
# HdfsReader encapsulates block reads from a HDFS file
# Initialized to start from any block, further blocks/bytes can be read from there.
type HdfsReader <: MapInputReader
    url::String
    begin_blk::Int
    cv::ChainedVector{Uint8}
    fs::HdfsFS
    fi::HdfsFile
    finfo::HdfsFileInfo

    function HdfsReader(url::String="")
        r = new("", 0, ChainedVector{Uint8}())
        (length(url) > 0) && reset_pos(r, url)
        r
    end
end


function read_into_buff(r::HdfsReader, start_pos::Integer, buff::Vector{Uint8}, bytes::Integer) 
    hdfs_pread(r.fs, r.fi, convert(Int64, start_pos), convert(Ptr{Void}, buff), bytes) 
    buff
end

function read_block_buff(r::HdfsReader, blk::Int, bytes::Int=0)
    (0 == bytes) && (bytes = file_block_sz(r))

    start_pos = file_block_sz(r)*(blk-1)
    bytes = min(bytes, file_block_sz(r), r.finfo.size-start_pos)

    buff = Array(Uint8, bytes)
    read_into_buff(r, start_pos, buff, bytes)
end
read_next(r::HdfsReader, bytes::Int) = push!(r.cv, read_block_buff(r, r.begin_blk+1, bytes))

# reset the current position to blk, discarding earlier data and reusing the buffer to read from blk
function reset_pos(r::HdfsReader, url::String)
    url, frag = urldefrag(url)
    if(url != r.url)
        # new source
        comps = urlparse(url)
        uname = username(comps)
        hname = hostname(comps)
        portnum = port(comps)
        fname = comps.url

        r.fs = (nothing == uname) ? hdfs_connect(hname, portnum) : hdfs_connect_as_user(hname, portnum, uname)
        r.fi = hdfs_open_file_read(r.fs, fname)
        r.finfo = hdfs_get_path_info(r.fs, fname)
        r.url = url
    end

    r.begin_blk = int(frag)
    bytes = block_sz(r)
    start_pos = file_block_sz(r)*(r.begin_blk-1)

    buff = (length(r.cv) > 0) ? shift!(r.cv) : Array(Uint8, bytes)
    (length(buff) != bytes) && resize!(buff, bytes)
    empty!(r.cv)                    # can put buffers back into a cache here
    push!(r.cv, read_into_buff(r, start_pos, buff, convert(Int, bytes)))
end

position(r::HdfsReader) = int64((r.begin_blk > 0) ? (file_block_sz(r)*(r.begin_blk-1) + r.cv.sz) : 0)
eof(r::HdfsReader) = (position(r) == r.finfo.size)
file_block_sz(r::HdfsReader) = r.finfo.block_sz
function block_sz(r::HdfsReader)
    bs = file_block_sz(r)
    start_pos = bs*(r.begin_blk-1)
    min(bs, r.finfo.size-start_pos)
end



##
# Iterator for HdfsReader using the find_rec function
type HdfsReaderIter <: MapInputIterator
    r::HdfsReader
    fn_find_rec::Function
    chunk_len::Int64
    rec::Union(Any,Nothing)
end

function iterator(r::HdfsReader, url::String, fn_find_rec::Function)
    reset_pos(r, url)
    chunk_len = block_sz(r)
    HdfsReaderIter(r, fn_find_rec, chunk_len, nothing)
end

start(iter::HdfsReaderIter) = iter.fn_find_rec(iter, 1)
done(iter::HdfsReaderIter, state) = (state > iter.chunk_len)
next(iter::HdfsReaderIter, state) = (iter.rec, iter.fn_find_rec(iter, state))

