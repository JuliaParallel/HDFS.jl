##
# HdfsReader
# A convenience type to encapsulate block reads from a HDFS file
# Can be initialized to start from any block.
# Can read further blocks/bytes from there.
# Is not designed to be used for streaming read, as it holds all data read in the buffer.
type HdfsReader
    fs::HdfsFS
    fi::HdfsFile
    finfo::HdfsFileInfo
    cv::ChainedVector{Uint8}
    begin_blk::Int

    function HdfsReader(fs::HdfsFS, fi::HdfsFile, finfo::HdfsFileInfo, blk::Int)
        r = new(fs, fi, finfo)
        (blk < 0) && (blk = 0)
        r.cv = (blk > 0) ? ChainedVector{Uint8}(read_block_buff(r, blk, finfo.block_sz)) : ChainedVector{Uint8}()
        r.begin_blk = blk
        r
    end
end


function read_into_buff(r::HdfsReader, start_pos::Int, buff::Vector{Uint8}, bytes::Int) 
    hdfs_pread(r.fs, r.fi, start_pos, convert(Ptr{Void}, buff), bytes) 
    buff
end

function read_block_buff(r::HdfsReader, blk::Int, bytes::Int=0)
    (0 == bytes) && (bytes = r.finfo.block_sz)

    start_pos = (r.finfo.block_sz)*(blk-1)
    bytes = min(bytes, r.finfo.block_sz, r.finfo.size-start_pos)

    buff = Array(Uint8, bytes)
    read_into_buff(r, start_pos, buff, bytes)
end
read_next(r::HdfsReader, bytes::Int) = push!(r.cv, read_block_buff(r, blk, bytes))

# reset the current position to blk, discarding earlier data and reusing the buffer to read from blk
function reset_pos(r::HdfsReader, blk::Int)
    bytes = r.finfo.block_sz
    start_pos = (r.finfo.block_sz)*(blk-1)
    bytes = min(bytes, r.finfo.block_sz, r.finfo.size-start_pos)

    buff = (length(r.cv) > 0) ? shift!(r.cv) : Array(Uint8, bytes)
    (length(buff) != bytes) && resize!(buff, bytes)
    r.begin_blk = blk
    push!(r.cv, read_into_buff(r, start_pos, buff, bytes))
end

position(r::HdfsReader) = (r.begin_blk > 0) ? ((r.finfo.block_sz)*(r.begin_blk-1) + r.cv.sz) : 0
eof(r::HdfsReader) = (position(r) == r.finfo.size)
    

