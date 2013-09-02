const def_file_blk_sz = (64*1024*1024)
##
# MapInputReader to read block sized dataframes
type FsBlockReader <: MapStreamInputReader
    fbr::BlockIO
    end_byte::Union(Char,Nothing)
    block_sz::Int
    function FsBlockReader(path::String="", end_byte::Union(Char,Nothing)=nothing, block_sz::Int=def_file_blk_sz)
        ret = new()
        ret.end_byte = end_byte
        ret.block_sz = block_sz
        !isempty(path) && reset_pos(ret, path)
        ret
    end
end
get_stream(fr::FsBlockReader) = fr.fbr
function reset_pos(fr::FsBlockReader, path::String)
    path,frag = urldefrag(path)
    f = open(path, "r")
    blksz = fr.block_sz
    blk = int(frag)
    begn = blksz*(blk-1)+1
    endn = min(filesize(path),blksz*blk)
    #println("processing block $blk range $begn : $endn")
    fr.fbr = BlockIO(f, begn:endn, fr.end_byte)
end


##
# Input for map
type MRFsFileInput <: MRInput
    source_spec
    reader_fn::Function

    file_list
    file_info
    block_sz::Int

    function MRFsFileInput(source_spec, reader_fn::Function, block_sz::Int=def_file_blk_sz)
        new(source_spec, reader_fn, nothing, nothing, block_sz)
    end
end

input_reader_type(inp::MRFsFileInput) = (MRFsFileInput, inp.block_sz)
get_input_reader(::Type{MRFsFileInput}, block_sz::Int) = FsBlockReader("", '\n', block_sz)

function expand_file_inputs(inp::MRFsFileInput)
    fl = ASCIIString[]
    infol = Stat[]
    rwild = r"[\*\[\?]"
    fspec = ""

    function get_files(path::String, pattern::Regex)
        function filt(fname::String)
            isdir(fname) && return false
            ismatch(pattern, basename(fname))
        end
        dir_list = readdir(path)
        filter(filt, map(x->joinpath(path,x),dir_list))
    end
   
    function add_file(f::String) 
        push!(fl, f)
        push!(infol, stat(f))
    end

    function add_dir(path::String, pattern::Regex=r".*")
        for filt_fname in get_files(path, pattern)
            add_file(filt_fname)
        end
    end

    for fspec in inp.source_spec
        if(ismatch(rwild, fspec))                                                           # a directory with wild card specification
            dirname,pattern = ('/' in fspec) ? rsplit(fspec, "/", 2) : ("", fspec)    # separate the directory path and wild card specification
            (isempty(pattern) || ismatch(rwild,dirname)) && error(string("wild card must be part of file name. invalid url: ", fspec))
            isempty(dirname) && (dirname = ".")
            add_dir(dirname, Regex(pattern))
        else
            isdir(fspec) ? add_dir(fspec) : add_file(fspec)        # can be a directory or a file
        end
    end
    inp.file_list = fl
    inp.file_info = infol
    inp
end

