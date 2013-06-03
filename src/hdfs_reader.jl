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
# Input for map
type MRFileInput <: MRInput
    source_spec
    reader_fn::Function

    file_list
    file_info
    file_blocks

    function MRFileInput(source_spec, reader_fn::Function)
        new(source_spec, reader_fn, nothing, nothing, nothing)
    end
end


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

