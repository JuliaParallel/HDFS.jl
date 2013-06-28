##
# MapInputReader to read block sized dataframes
type HdfsBlockReader <: MapStreamInputReader
    hbr::BlockIO
    end_byte::Union(Char,Nothing)
    function HdfsBlockReader(url::String="", end_byte::Union(Char,Nothing)=nothing)
        ret = new()
        ret.end_byte = end_byte
        !isempty(url) && reset_pos(ret, url)
        ret
    end
end
get_stream(hdfr::HdfsBlockReader) = hdfr.hbr
function reset_pos(hdfr::HdfsBlockReader, url::String)
    url,frag = urldefrag(url)
    f = hdfs_open(url, "r")
    blksz = stat(f).block_sz
    blk = int(frag)
    begn = blksz*(blk-1)+1
    endn = min(filesize(f),blksz*blk)
    #println("processing block $blk range $begn : $endn")
    hdfr.hbr = BlockIO(f, begn:endn, hdfr.end_byte)
end


##
# Input for map
type MRHdfsFileInput <: MRInput
    source_spec
    reader_fn::Function

    file_list
    file_info
    file_blocks

    function MRHdfsFileInput(source_spec, reader_fn::Function, rdr_type::String="")
        new(source_spec, reader_fn, nothing, nothing, nothing)
    end
end

# allowed types: buffer, stream
input_reader_type(inp::MRHdfsFileInput) = (MRHdfsFileInput, "")
get_input_reader(::Type{MRHdfsFileInput}, rdr_typ::String) = HdfsBlockReader("", '\n')

function expand_file_inputs(inp::MRHdfsFileInput)
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

