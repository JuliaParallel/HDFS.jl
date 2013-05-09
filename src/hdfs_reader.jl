##
# HdfsReader
# A convenience type to encapsulate block reads from a HDFS file
# Can be initialized to start from any block.
# Can read further blocks/bytes from there.
# Is not designed to be used for streaming read, as it holds all data read in the buffer.
type HdfsReader
    url::String
    begin_blk::Int
    cv::ChainedVector{Uint8}
    fs::HdfsFS
    fi::HdfsFile
    finfo::HdfsFileInfo

    function HdfsReader(url::String="", blk::Int=0)
        r = new("", 0, ChainedVector{Uint8}())
        (length(url) > 0) && (blk > 0) && reset_pos(r, url, blk)
        r
    end
end


function read_into_buff(r::HdfsReader, start_pos::Integer, buff::Vector{Uint8}, bytes::Integer) 
    hdfs_pread(r.fs, r.fi, convert(Int64, start_pos), convert(Ptr{Void}, buff), bytes) 
    buff
end

function read_block_buff(r::HdfsReader, blk::Int, bytes::Int=0)
    (0 == bytes) && (bytes = block_sz(r))

    start_pos = block_sz(r)*(blk-1)
    bytes = min(bytes, block_sz(r), r.finfo.size-start_pos)

    buff = Array(Uint8, bytes)
    read_into_buff(r, start_pos, buff, bytes)
end
read_next(r::HdfsReader, bytes::Int) = push!(r.cv, read_block_buff(r, r.begin_blk+1, bytes))

# reset the current position to blk, discarding earlier data and reusing the buffer to read from blk
function reset_pos(r::HdfsReader, url::String, blk::Int)
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

    bytes = block_sz(r, blk)
    start_pos = block_sz(r)*(blk-1)

    buff = (length(r.cv) > 0) ? shift!(r.cv) : Array(Uint8, bytes)
    (length(buff) != bytes) && resize!(buff, bytes)
    r.begin_blk = blk
    empty!(r.cv)                    # can put buffers back into a cache here
    push!(r.cv, read_into_buff(r, start_pos, buff, convert(Int, bytes)))
end

position(r::HdfsReader) = int64((r.begin_blk > 0) ? (block_sz(r)*(r.begin_blk-1) + r.cv.sz) : 0)
eof(r::HdfsReader) = (position(r) == r.finfo.size)
block_sz(r::HdfsReader) = r.finfo.block_sz
function block_sz(r::HdfsReader, blk::Int)
    bs = block_sz(r)
    start_pos = bs*(blk-1)
    min(bs, r.finfo.size-start_pos)
end

