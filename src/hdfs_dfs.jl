
const hdfs_fsstore = Dict{@compat(Tuple{AbstractString,Integer,AbstractString}), Vector{Any}}()
typealias IPv4v6 Union(IPv4,IPv6)

function finalize_hdfs_fs(fs::HdfsFS) 
    (C_NULL == fs.ptr) && return
    key, arr = _get_ptr_ref(fs.host, fs.port, fs.user, false)
    if((arr[1] -= 1) == 0)
        ccall((:hdfsDisconnect, _libhdfs), Int32, (Ptr{Void},), fs.ptr) 
        arr[2] = fs.ptr = C_NULL
        # TODO: remove from hdfs_fsstore to save space
    end
end

# hdfs_connect returns pointers to the same handle across multiple calls for the same file system
# so we must have an abstraction with reference count
function _get_ptr_ref(host::AbstractString, port::Integer, user::AbstractString="", incr::Bool=true)
    key = (host, port, user)
    if(haskey(hdfs_fsstore, key))
        arr = hdfs_fsstore[key]
        if(arr[1] > 0)
            incr && (arr[1] += 1)
            return key, arr
        end
    end
    key, [0, C_NULL]
end

function hdfs_connect(host::AbstractString="default", port::Integer=0, user::AbstractString="") 
    key, arr = _get_ptr_ref(host, port, user)
    (0 != arr[1]) && return HdfsFS(host, @compat(Int(port)), user, arr[2])
    porti32 = @compat Int32(port)
    ptr = (user == "") ? 
            ccall((:hdfsConnect, _libhdfs), Ptr{Void}, (Ptr{UInt8}, Int32), bytestring(host), porti32) : 
            ccall((:hdfsConnectAsUser, _libhdfs), Ptr{Void}, (Ptr{UInt8}, Int32, Ptr{UInt8}), bytestring(host), porti32, bytestring(user))
    (C_NULL == ptr) && error("hdfs connect failed")
    hdfs_fsstore[key] = [1, ptr]
    HdfsFS(host, @compat(Int(port)), "", ptr)
end

function hdfs_connect(url::HdfsURL)
    uri = URI(url.url)
    if ':' in uri.userinfo
        uname,_passwd = userinfo(uri.userinfo)
    else
        uname = uri.userinfo
        _passwd = ""
    end
    hname = uri.host
    portnum = uri.port
    hdfs_connect(hname, portnum, (nothing == uname)?"":uname)
end


# file control flags need to be done manually
function hdfs_open(fs::HdfsFS, path::AbstractString, mode::AbstractString, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)
    flags = -1
    (mode == "r") && (flags = Base.JL_O_RDONLY)
    (mode == "w") && (flags = Base.JL_O_WRONLY)
    (mode == "a") && (flags = (Base.JL_O_WRONLY | Base.JL_O_APPEND))
    (flags == -1) && error("unknown open mode $(mode)")

    file = ccall((:hdfsOpenFile, _libhdfs), Ptr{Void}, (Ptr{Void}, Ptr{UInt8}, Int32, Int32, Int16, Int32), fs.ptr, bytestring(path),
        convert(Int32, flags),
        convert(Int32, buffer_sz),
        convert(Int16, replication),
        convert(Int32, bsz))
    (C_NULL == file) && error("error opening file $(path)")
    return HdfsFile(fs, path, file)
end

function hdfs_open(f::HdfsFile, mode::AbstractString, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)
    (C_NULL != f.ptr) && (0 != hdfs_close(f)) && error("error closing file")
    fnew = hdfs_open(f.fs, f.path, mode, buffer_sz, replication, bsz)
    f.ptr = fnew.ptr
    fnew.ptr = C_NULL
    f
end

function hdfs_open(url::AbstractString, mode::AbstractString, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)
    uri = defrag(URI(url))
    (uri.schema != "hdfs") && error("not a HDFS URL")
    uname,_passwd = userinfo(uri.userinfo)
    hname = uri.host
    portnum = uri.port
    fname = uri.path

    fs = hdfs_connect(hname, portnum, (nothing == uname)?"":uname)
    hdfs_open(fs, fname, mode, buffer_sz, replication, bsz)
end
hdfs_open(url::HdfsURL, mode::AbstractString, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0) = hdfs_open(url.url, mode, buffer_sz, replication, bsz)


function hdfs_close(file::HdfsFile) 
    ret = (file.fs.ptr == C_NULL) ? 0 : ccall((:hdfsCloseFile, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}), file.fs.ptr, file.ptr)
    (0 == ret) && (file.ptr = C_NULL)
    ret
end

function hdfs_exists(fs::HdfsFS, path::AbstractString) 
    ret = ccall((:hdfsExists, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}), fs.ptr, bytestring(path))
    # TODO: examine why hdfsExists returns -1 when file does not exist. as per docs it should return 1
    #(ret < 0) && println("error checking file $(path) exists. error: $(ret)")
    (ret == 0)
end

hdfs_seek(file::HdfsFile, desired_pos) = ccall((:hdfsSeek, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Int64), file.fs.ptr, file.ptr, convert(Int64,desired_pos))

hdfs_tell(file::HdfsFile) = ccall((:hdfsTell, _libhdfs), Int64, (Ptr{Void}, Ptr{Void}), file.fs.ptr, file.ptr)

hdfs_read(file::HdfsFile, buff::Ptr, len::Integer) = ccall((:hdfsRead, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Ptr{Void}, Int32), file.fs.ptr, file.ptr, buff, convert(Int32,len))
function hdfs_read(file::HdfsFile, len::Integer)
    buff = Array(UInt8, len) 
    (-1 == (r = hdfs_read(file, convert(Ptr{Void}, pointer(buff)), len))) && error("error reading file: -1")
    (buff, r)
end

hdfs_pread(file::HdfsFile, position::Int64, buff::Ptr, len::Integer) = ccall((:hdfsPread, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Int64, Ptr{Void}, Int32), file.fs.ptr, file.ptr, position, buff, len)
function hdfs_pread(file::HdfsFile, position::Int64, len::Integer)
    buff = Array(UInt8, len) 
    (-1 == (r = hdfs_pread(file, position, buff, len))) && error("error reading file: -1")
    (buff, r)
end

#can be passed an ASCIIString (length not necessary in that case)
hdfs_write(file::HdfsFile, buff::Ptr, len::Integer) = ccall((:hdfsWrite, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Ptr{Void}, Int32), file.fs.ptr, file.ptr, buff, convert(Int32,len))
hdfs_write(file::HdfsFile, buff::ASCIIString, len::Integer) = hdfs_write(file, convert(Ptr{Void}, pointer(buff.data)), len)
hdfs_write(file::HdfsFile, buff::ASCIIString) = hdfs_write(file, buff, length(buff))

hdfs_flush(file::HdfsFile) = ccall((:hdfsFlush, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}), file.fs.ptr, file.ptr)

hdfs_available(file::HdfsFile) = ccall((:hdfsAvailable, _libhdfs), Int32, (Ptr{Void},Ptr{Void}), file.fs.ptr, file.ptr)

hdfs_copy(srcFS::HdfsFS, src::AbstractString, dstFS::HdfsFS, dst::AbstractString) = ccall((:hdfsCopy, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, Ptr{Void}, Ptr{UInt8}), srcFS.ptr, bytestring(src), dstFS.ptr, bytestring(dst))

hdfs_move(srcFS::HdfsFS, src::AbstractString, dstFS::HdfsFS, dst::AbstractString) = ccall((:hdfsMove, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, Ptr{Void}, Ptr{UInt8}), srcFS.ptr, bytestring(src), dstFS.ptr, bytestring(dst))

hdfs_delete(fs::HdfsFS, path::AbstractString) = ccall((:hdfsDelete, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}), fs.ptr, bytestring(path))

hdfs_rename(fs::HdfsFS, old_path::AbstractString, new_path::AbstractString) = ccall((:hdfsRename, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, Ptr{UInt8}), fs.ptr, bytestring(old_path), bytestring(new_path))

function hdfs_pwd(fs::HdfsFS, buff::Vector{UInt8}) 
    ptr = ccall((:hdfsGetWorkingDirectory, _libhdfs), Ptr{UInt8}, (Ptr{Void}, Ptr{UInt8}, Int32), fs.ptr, buff, convert(Int32,length(buff)))
    (C_NULL == ptr) && error("error getting working directory")
    len = ccall((:strlen, "libc"), UInt, (Ptr{UInt8},), buff)
    ASCIIString(buff[1:len])
end
hdfs_pwd(fs::HdfsFS, buff_sz::Integer) = hdfs_pwd(fs, Array(UInt8, buff_sz))
hdfs_pwd(fs::HdfsFS) = hdfs_pwd(fs, 1024)

hdfs_cd(fs::HdfsFS, path::AbstractString) = ccall((:hdfsSetWorkingDirectory, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}), fs.ptr, bytestring(path))

hdfs_mkdir(fs::HdfsFS, path::AbstractString) = ccall((:hdfsCreateDirectory, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}), fs.ptr, bytestring(path))

hdfs_set_replication(fs::HdfsFS, path::AbstractString, replication::Integer)=ccall((:hdfsSetReplication, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, Int16), fs.ptr, bytestring(path), @compat(Int16(replication)))

function hdfs_list_directory(fs::HdfsFS, path::AbstractString)
    num_entries = Int32[-1]
    info_ptr = ccall((:hdfsListDirectory, _libhdfs), Ptr{c_hdfsfileinfo}, (Ptr{Void}, Ptr{UInt8}, Ptr{Int32}), fs.ptr, bytestring(path), num_entries)
    (0 == num_entries[1]) && (return [])
    (C_NULL == info_ptr) && error(string("Error listing path ", path))

    n = @compat Int(num_entries[1])
    ret = [HdfsFileInfo(x) for x in pointer_to_array(info_ptr, (n,))]
    ccall((:hdfsFreeFileInfo, _libhdfs), Void, (Ptr{Void}, Int32), info_ptr, num_entries[1])
    ret
end

function hdfs_get_path_info(fs::HdfsFS, path::AbstractString)
    info_ptr = ccall((:hdfsGetPathInfo, _libhdfs), Ptr{c_hdfsfileinfo}, (Ptr{Void}, Ptr{UInt8}), fs.ptr, bytestring(path))
    (C_NULL == info_ptr) && error(string("Error getting path ", path))

    ret = HdfsFileInfo(info_ptr)
    ccall((:hdfsFreeFileInfo, _libhdfs), Void, (Ptr{Void}, Int32), info_ptr, 1)
    ret
end

hdfs_is_directory(fs::HdfsFS, path::AbstractString) = (hdfs_get_path_info(fs,path).kind == HDFS_OBJ_DIR)

function hdfs_blocks(fs::HdfsFS, path::AbstractString, start::Integer=1, len::Integer=0, as_ip::Bool=false)
    (len == 0) && (len = hdfs_get_path_info(fs, path).size)
    c_ptr = ccall((:hdfsGetHosts, _libhdfs), Ptr{Ptr{Ptr{UInt8}}}, (Ptr{Void}, Ptr{UInt8}, Int64, Int64), fs.ptr, bytestring(path), 
                    convert(Int64,start), 
                    convert(Int64,len))
    (C_NULL == c_ptr) && error("Error getting hosts for file $(path)")

    i = 1
    ret_vals = Array(as_ip ? Array{IPv4v6,1} : Array{ASCIIString,1}, 0)
    while true
        h_list = unsafe_load(c_ptr, i)
        (h_list == C_NULL) && break
        
        j = 1
        arr = Array(as_ip ? IPv4v6 : ASCIIString, 0)
        while true
            hname = unsafe_load(h_list, j)
            (hname == C_NULL) && break
            shname = bytestring(hname)
            push!(arr, as_ip ? getaddrinfo(shname) : shname)
            #print(bytestring(hname), ", ")
            j += 1
        end
        push!(ret_vals, arr)
        #println("")
        i += 1
    end

    ccall((:hdfsFreeHosts, _libhdfs), Void, (Ptr{Ptr{Ptr{UInt8}}},), c_ptr)
    ret_vals
end
hdfs_blocks(f::HdfsFile, start::Integer=1, len::Integer=0, as_ip::Bool=false) = hdfs_blocks(f.fs, f.path, start, len, as_ip)
hdfs_blocks(url::HdfsURL, start::Integer=1, len::Integer=0, as_ip::Bool=false) = hdfs_blocks(hdfs_connect(url), URI(url.url).path, start, len, as_ip)


hdfs_get_default_block_size(fs::HdfsFS) = ccall((:hdfsGetDefaultBlockSize, _libhdfs), Int64, (Ptr{Void},), fs.ptr)

hdfs_get_capacity(fs::HdfsFS) = ccall((:hdfsGetCapacity, _libhdfs), Int64, (Ptr{Void},), fs.ptr)

hdfs_get_used(fs::HdfsFS) = ccall((:hdfsGetUsed, _libhdfs), Int64, (Ptr{Void},), fs.ptr)

hdfs_chown(fs::HdfsFS, path::AbstractString, owner::AbstractString, group::AbstractString) = ccall((:hdfsChown, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, Ptr{UInt8}, Ptr{UInt8}), fs.ptr, bytestring(path), bytestring(owner), bytestring(group))

hdfs_chmod(fs::HdfsFS, path::AbstractString, mode::Int16) = ccall((:hdfsChmod, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, Int16), fs.ptr, bytestring(path), mode)

hdfs_utime(fs::HdfsFS, path::AbstractString, mtime::Integer, atime::Integer) = ccall((:hdfsUtime, _libhdfs), Int32, (Ptr{Void}, Ptr{UInt8}, TimeT, TimeT), fs.ptr, path, convert(TimeT, mtime), convert(TimeT, atime))

isequal(u1::HdfsURL, u2::HdfsURL) = isequal(u1.url, u2.url)
==(u1::HdfsURL, u2::HdfsURL) = (u1.url == u2.url)
hash(u::HdfsURL) = hash(u.url)


##
# File system implementation for HdfsFS
pwd(fs::HdfsFS) = URI(hdfs_pwd(fs)).path
function cd(fs::HdfsFS, path::AbstractString)
    hdfs_exists(fs, path) && (0 == hdfs_cd(fs, path)) && (return nothing)
    error("no such file or directory $path")
end
mkdir(fs::HdfsFS, path::AbstractString) = (0 == hdfs_mkdir(fs, path)) ? nothing : error("error creating directory $path")
mv(srcFS::HdfsFS, src::AbstractString, dstFS::HdfsFS, dst::AbstractString) = (srcFS == dstFS) ? mv(srcFS, src, dst) : (0 == hdfs_move(srcFS, src, dstFS, dst)) ? nothing : error("error moving file")
mv(fs::HdfsFS, src::AbstractString, dst::AbstractString) = (0 == hdfs_rename(fs, src, dst)) ? nothing : error("error renaming file")
rm(fs::HdfsFS, path::AbstractString) = (0 == hdfs_delete(fs, path)) ? nothing : error("error deleting $path")
cp(srcFS::HdfsFS, src::AbstractString, dstFS::HdfsFS, dst::AbstractString) = (0 == hdfs_copy(srcFS, src, dstFS, dst)) ? nothing : error("error copying file")
cp(fs::HdfsFS, src::AbstractString, dst::AbstractString) = cp(fs, src, fs, dst)
rmdir(fs::HdfsFS, path::AbstractString) = rm(fs, path)
function readdir(fs::HdfsFS) 
    offset = length(pwd(fs))+2
    AbstractString[URI(fi.name).path[offset:end] for fi in hdfs_list_directory(fs, ".")]
end
function readdir(fs::HdfsFS, path::AbstractString)
    (path == ".") && (return readdir(fs))
    p = pwd(fs)
    cd(fs, path)
    ret = readdir(fs)
    cd(fs, p)
    ret
end

isdir(fs::HdfsFS, path::AbstractString) = hdfs_is_directory(fs, path)
function isdir(f::HdfsURL)
    uri = URI(f.url)
    isdir(hdfs_connect(f), uri.path)
end

##
# IO implementation for HdfsFile
# this is crap. should implement the array interface also
open(fs::HdfsFS, path::AbstractString, mode::AbstractString="r", buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0) = hdfs_open(fs, path, mode, buffer_sz, replication, bsz)
open(f::HdfsFile, mode::AbstractString="r", buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0) = hdfs_open(f, mode, buffer_sz, replication, bsz)
open(url::HdfsURL, mode::AbstractString="r", buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0) = hdfs_open(url.url, mode, buffer_sz, replication, bsz)
close(f::HdfsFile) = hdfs_close(f)
eof(f::HdfsFile) = (position(f) == filesize(f))

const _sb = Array(UInt8, 1)
read(f::HdfsFile, x::Type{UInt8}) = read(f, _sb)[1]
function read{T}(f::HdfsFile, a::Array{T})
    remaining = length(a)*sizeof(T)
    avlb = nb_available(f)
    while((remaining > 0) && (avlb > 0))
        ret = hdfs_read(f, pointer(a, length(a)-remaining+1), min(remaining, avlb))
        remaining -= ret
        avlb -= ret
    end
    (remaining > 0) && throw(EOFError())
    a
end
readbytes(f::HdfsFile, nb::Integer) = bytestring(read(f, Array(UInt8, nb)))
readall(f::HdfsFile) = readbytes(f, nb_available(f))

function peek(f::HdfsFile)
    eof(f) && (return 0xff)
    ret = read(f, UInt8)
    skip(f, -1)
    ret
end

write(f::HdfsFile, p::Ptr, nb::Integer) = write(f, p, convert(Int,nb))
write(f::HdfsFile, p::Ptr, nb::Int) = hdfs_write(f, p, nb)
write(f::HdfsFile, x::UInt8) = write(f, UInt8[x])
write{T}(f::HdfsFile, a::Array{T}, len) = write_sub(f, a, 1, length(a))
write{T}(f::HdfsFile, a::Array{T}) = write(f, a, length(a))
write_sub{T}(f::HdfsFile, a::Array{T}, offs, len) = isbits(T) ? write(f, pointer(a,offs), len*sizeof(T)) : error("$T is not bits type")

flush(f::HdfsFile) = (0 == hdfs_flush(f)) ? nothing : error("error flushing")
nb_available(f::HdfsFile) = hdfs_available(f)
function position(f::HdfsFile)
    p = hdfs_tell(f)
    (p >= 0) ? p : error("error getting current position")
end

stat(fs::HdfsFS, path::AbstractString) = hdfs_get_path_info(fs, path)
stat(f::HdfsFile) = stat(f.fs, f.path)
stat(url::HdfsURL) = stat(hdfs_connect(url), URI(url.url).path)

filesize(fs::HdfsFS, path::AbstractString) = (stat(fs,path)).size
filesize(f::HdfsFile) = filesize(f.fs, f.path)
filesize(url::HdfsURL) = filesize(hdfs_connect(url), URI(url.url).path)

seek(f::HdfsFile, n::Integer) = ((n >= 0) ? (0 == hdfs_seek(f, n)) : error("invalid position"))
seekend(f::HdfsFile) = seek(f, filesize(f))
seekstart(f::HdfsFile) = seek(f, 0)
skip(f::HdfsFile, n::Integer) = seek(f, n+position(f))

