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
        !isempty(url) && reset_pos(r, url)
        r
    end
end

function read_into_buff(r::HdfsReader, start_pos::Integer, buff::Vector{Uint8}, bytes::Integer) 
    hdfs_pread(r.fi, convert(Int64, start_pos), convert(Ptr{Void}, buff), bytes) 
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
function read_next_blk(r::HdfsReader)
    bytes_read = r.cv.sz
    blks_read = bytes_read/file_block_sz(r)
    iblks_read = int(floor(blks_read))
    (iblks_read != blks_read) && error("cant read block after a part block read")
    push!(r.cv, read_block_buff(r, r.begin_blk+iblks_read, file_block_sz(r)))
end

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

        r.fs = hdfs_connect(hname, portnum, (nothing == uname)?"":uname)
        r.fi = hdfs_open(r.fs, fname, "r")
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
    nothing
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
# IO interface for a file block reader
# block_read_beyond
#   == 0: the block reader will read through all the blocks
#       TODO: have a way to reuse buffers from beginning of the chain
#   > 0: read the specified bytes and stop
#   < 0: do not read beyond
# 
type HdfsBlockStream <: IO
    hrdr::HdfsReader
    pos::Int
    start_pos::Int
    eof_pos::Int
    end_byte::Uint8
    max_block_overflow::Int

    function HdfsBlockStream(url::String="", end_byte::Char='\n', max_block_overflow::Int=0)
        hrdr = HdfsReader("")
        hbr = new(hrdr, 0, 1, typemax(Int), end_byte, max_block_overflow)
        !isempty(url) && reset_pos(hbr, url)
        hbr
    end
end

nb_available(s::HdfsBlockStream) = (length(s.hrdr.cv) - s.pos)
position(s::HdfsBlockStream) = s.pos
seekstart(s::HdfsBlockStream) = seek(s, s.start_pos)
seekend(s::HdfsBlockStream) = seek(s, min(length(s.hrdr.cv), (s.eof_pos == typemax(Int)) ? s.eof_pos : s.eof_pos+1))
skip(s::HdfsBlockStream, offset) = seek(s, s.pos+offset)
seek(s::HdfsBlockStream, pos) = (s.pos = (s.start_pos <= pos <= s.eof_pos) ? pos : error("seek to $pos failed"))
function peek(s::HdfsBlockStream)
    ret = read(s, Uint8)
    seek(s, position(s)-1)
    ret
end
function find_end_pos(s::HdfsBlockStream)
    seekend(s)
    while(!eof(s) && (s.end_byte != read(s, Uint8)))
        continue 
    end
    s.eof_pos = position(s)-1
end
function find_start_pos(s::HdfsBlockStream)
    (s.hrdr.begin_blk == 1) && (return (s.start_pos = 0))
    seekstart(s)
    while(s.end_byte != read(s, Uint8)) continue end
    s.start_pos = position(s)
end
function reset_pos(s::HdfsBlockStream, url::String)
    reset_pos(s.hrdr, url)
    s.eof_pos = eof(s.hrdr) ? length(s.hrdr.cv) : typemax(Int)
    find_start_pos(s)
    find_end_pos(s)
    #println("$url : $(s.start_pos) - $(s.eof_pos)")
    seekstart(s)
    nothing
end
function read(s::HdfsBlockStream, x::Type{Uint8})
    hrdr = s.hrdr
    eof(s) && throw(EOFError())
    s.pos = s.pos+1
    if(0 > nb_available(s))
        #println("reading next $(s.max_block_overflow) bytes")
        if(s.max_block_overflow > 0)
            read_next(hrdr, s.max_block_overflow)
            s.eof_pos = hrdr.cv.sz
        else
            read_next_blk(hrdr)
            eof(hrdr) && (s.eof_pos = hrdr.cv.sz)
        end
    end
    hrdr.cv[s.pos]
end
eof(s::HdfsBlockStream) = (s.pos >= s.eof_pos)


##
# MapInputReader to read block sized dataframes
type HdfsBlockReader <: MapInputReader
    hbr::HdfsBlockStream
    HdfsBlockReader(url::String="", end_byte::Char='\n', max_block_overflow::Int=0) = new(HdfsBlockStream(url, end_byte, max_block_overflow))
end
get_stream(hdfr::HdfsBlockReader) = hdfr.hbr
reset_pos(hdfr::HdfsBlockReader, url::String) = reset_pos(hdfr.hbr, url)


##
# Input for map
type MRFileInput <: MRInput
    source_spec
    reader_fn::Function
    input_reader_type::String

    file_list
    file_info
    file_blocks

    function MRFileInput(source_spec, reader_fn::Function, input_reader_type::String="buffer")
        new(source_spec, reader_fn, input_reader_type, nothing, nothing, nothing)
    end
end

# allowed types: buffer, stream
input_reader_type(inp::MRFileInput) = (MRFileInput, inp.input_reader_type)
get_input_reader(::Type{MRFileInput}, rdr_typ::String) = ((rdr_typ == "buffer") ? HdfsReader() : (rdr_typ == "stream") ? HdfsBlockReader() : error("unknown type $rdr_typ"))

function expand_file_inputs(inp::MRFileInput)
    fl = ASCIIString[]
    infol = HdfsFileInfo[]
    blockl = {}
    rwild = r"[\*\[\?]"
    fspec = ""

    function is_directory(comps::URLComponents)
        uname = username(comps)
        hdfs_is_directory(hdfs_connect(hostname(comps), port(comps), (nothing == uname)?"":uname), comps.url)
    end

    function get_files(comps::URLComponents, pattern::Regex)
        function filt(filt_finfo::HdfsFileInfo)
            (filt_finfo.kind != HDFS_OBJ_FILE) && return false
            filt_fcomps = urlparse(filt_finfo.name)
            filt_dir,file_fname = rsplit(filt_fcomps.url, "/", 2)
            ismatch(pattern, file_fname)
        end
        uname = username(comps)
        fs = hdfs_connect(hostname(comps), port(comps), (nothing == uname)?"":uname)
        dir_list = hdfs_list_directory(fs, comps.url).arr
        map(x->x.name, filter(filt, dir_list))
    end
   
    function add_file(f::String) 
        up = urlparse(f)
        uname = username(up)
        fname = up.url
        fs = hdfs_connect(hostname(up), port(up), (nothing == uname) ? "" : uname)
        finfo = hdfs_get_path_info(fs, fname)
        blocks = hdfs_blocks(fs, fname, 0, finfo.size)
        
        push!(fl, f)
        push!(infol, finfo)
        push!(blockl, blocks)
    end

    function add_file(up::URLComponents, pattern::Regex=r".*")
        for filt_fname in get_files(up, pattern)
            add_file(filt_fname)
        end
    end

    for fspec in inp.source_spec
        up = urlparse(fspec)
        if(ismatch(rwild, fspec))                                   # a directory with wild card specification
            url,pattern = rsplit(up.url, "/", 2)                    # separate the directory path and wild card specification
            ((pattern == "") || ismatch(rwild,url)) && error(string("wild card must be part of file name. invalid url: ", fspec))
            up_dir = copy(up)
            up_dir.url = (url == "") ? "/" : url
            add_file(up_dir, Regex(pattern))
        else
            is_directory(up) ? add_file(up) : add_file(fspec)        # can be a directory or a file
        end
    end
    inp.file_list = fl
    inp.file_info = infol
    inp.file_blocks = blockl
    inp
end

