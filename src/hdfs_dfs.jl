
const hdfs_fsstore = Dict{(String, Integer, String), Vector{Any}}()

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
function _get_ptr_ref(host::String, port::Integer, user::String="", incr::Bool=true)
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

function hdfs_connect(host::String="default", port::Integer=0, user::String="") 
    key, arr = _get_ptr_ref(host, port, user)
    (0 != arr[1]) && return HdfsFS(host, port, user, arr[2])
    ptr = (user == "") ? 
            ccall((:hdfsConnect, _libhdfs), Ptr{Void}, (Ptr{Uint8}, Int32), bytestring(host), int32(port)) : 
            ccall((:hdfsConnectAsUser, _libhdfs), Ptr{Void}, (Ptr{Uint8}, Int32, Ptr{Uint8}), bytestring(host), int32(port), bytestring(user))
    (C_NULL == ptr) && error("hdfs connect failed")
    hdfs_fsstore[key] = [1, ptr]
    HdfsFS(host, port, "", ptr)
end


# file control flags need to be done manually
function hdfs_open(fs::HdfsFS, path::String, mode::String, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)
    flags = -1
    (mode == "r") && (flags = Base.JL_O_RDONLY)
    (mode == "w") && (flags = Base.JL_O_WRONLY)
    (mode == "a") && (flags = (Base.JL_O_WRONLY | Base.JL_O_APPEND))
    (flags == -1) && error("unknown open mode $(mode)")

    file = ccall((:hdfsOpenFile, _libhdfs), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Int32, Int32, Int16, Int32), fs.ptr, bytestring(path), int32(flags), int32(buffer_sz), int16(replication), int32(bsz))
    (C_NULL == file) && error("error opening file $(path)")
    return HdfsFile(fs, path, file)
end

function hdfs_open(f::HdfsFile, mode::String, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)
    (C_NULL != f.ptr) && (0 != hdfs_close(f)) && error("error closing file")
    fnew = hdfs_open(f.fs, f.path, mode, buffer_sz, replication, bsz)
    f.ptr = fnew.ptr
    fnew.ptr = C_NULL
    f
end

function hdfs_close(file::HdfsFile) 
    ret = ccall((:hdfsCloseFile, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}), file.fs.ptr, file.ptr)
    (0 == ret) && (file.ptr = C_NULL)
    ret
end

function hdfs_exists(fs::HdfsFS, path::String) 
    ret = ccall((:hdfsExists, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))
    # TODO: examine why hdfsExists returns -1 when file does not exist. as per docs it should return 1
    #(ret < 0) && println("error checking file $(path) exists. error: $(ret)")
    (ret == 0)
end

hdfs_seek(file::HdfsFile, desired_pos) = ccall((:hdfsSeek, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Int64), file.fs.ptr, file.ptr, int64(desired_pos))

hdfs_tell(file::HdfsFile) = ccall((:hdfsTell, _libhdfs), Int64, (Ptr{Void}, Ptr{Void}), file.fs.ptr, file.ptr)

hdfs_read(file::HdfsFile, buff::Ptr, len::Integer) = ccall((:hdfsRead, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Ptr{Void}, Int32), file.fs.ptr, file.ptr, buff, int32(len))
function hdfs_read(file::HdfsFile, len::Integer)
    buff = Array(Uint8, len) 
    (-1 == (r = hdfs_read(file, convert(Ptr{Void}, buff), len))) && error("error reading file: -1")
    (buff, r)
end

hdfs_pread(file::HdfsFile, position::Int64, buff::Ptr, len::Integer) = ccall((:hdfsPread, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Int64, Ptr{Void}, Int32), file.fs.ptr, file.ptr, position, buff, len)
function hdfs_pread(file::HdfsFile, position::Int64, len::Integer)
    buff = Array(Uint8, len) 
    (-1 == (r = hdfs_pread(file, position, buff, len))) && error("error reading file: -1")
    (buff, r)
end

#can be passed an ASCIIString (length not necessary in that case)
hdfs_write(file::HdfsFile, buff::Ptr, len::Integer) = ccall((:hdfsWrite, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Ptr{Void}, Int32), file.fs.ptr, file.ptr, buff, int32(len))
hdfs_write(file::HdfsFile, buff::ASCIIString, len::Integer) = hdfs_write(file, convert(Ptr{Void}, buff.data), len)
hdfs_write(file::HdfsFile, buff::ASCIIString) = hdfs_write(file, bytestring(buff), length(buff))

hdfs_flush(file::HdfsFile) = ccall((:hdfsFlush, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}), file.fs.ptr, file.ptr)

hdfs_available(file::HdfsFile) = ccall((:hdfsAvailable, _libhdfs), Int32, (Ptr{Void},Ptr{Void}), file.fs.ptr, file.ptr)

hdfs_copy(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String) = ccall((:hdfsCopy, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Void}, Ptr{Uint8}), srcFS.ptr, bytestring(src), dstFS.ptr, bytestring(dst))

hdfs_move(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String) = ccall((:hdfsMove, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Void}, Ptr{Uint8}), srcFS.ptr, bytestring(src), dstFS.ptr, bytestring(dst))

hdfs_delete(fs::HdfsFS, path::String) = ccall((:hdfsDelete, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_rename(fs::HdfsFS, old_path::String, new_path::String) = ccall((:hdfsRename, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Uint8}), fs.ptr, bytestring(old_path), bytestring(new_path))

function hdfs_pwd(fs::HdfsFS, buff::Vector{Uint8}) 
    ptr = ccall((:hdfsGetWorkingDirectory, _libhdfs), Ptr{Uint8}, (Ptr{Void}, Ptr{Uint8}, Int32), fs.ptr, buff, int32(length(buff)))
    (C_NULL == ptr) && error("error getting working directory")
    len = ccall((:strlen, "libc"), Uint, (Ptr{Uint8},), buff)
    ASCIIString(buff[1:len])
end
hdfs_pwd(fs::HdfsFS, buff_sz::Integer) = hdfs_pwd(fs, Array(Uint8, buff_sz))
hdfs_pwd(fs::HdfsFS) = hdfs_pwd(fs, 1024)

hdfs_cd(fs::HdfsFS, path::String) = ccall((:hdfsSetWorkingDirectory, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_mkdir(fs::HdfsFS, path::String) = ccall((:hdfsCreateDirectory, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_set_replication(fs::HdfsFS, path::String, replication::Integer)=ccall((:hdfsSetReplication, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Int16), fs.ptr, bytestring(path), int16(replication))

function hdfs_list_directory(fs::HdfsFS, path::String)
    num_entries = Int32[-1]
    info_ptr = ccall((:hdfsListDirectory, _libhdfs), Ptr{c_hdfsfileinfo}, (Ptr{Void}, Ptr{Uint8}, Ptr{Int32}), fs.ptr, bytestring(path), num_entries)
    (0 == num_entries[1]) && (return [])
    (C_NULL == info_ptr) && error(string("Error listing path ", path))

    ret = [HdfsFileInfo(x) for x in pointer_to_array(info_ptr, (int(num_entries[1]),))]
    ccall((:hdfsFreeFileInfo, _libhdfs), Void, (Ptr{Void}, Int32), info_ptr, num_entries[1])
    ret
end

function hdfs_get_path_info(fs::HdfsFS, path::String)
    info_ptr = ccall((:hdfsGetPathInfo, _libhdfs), Ptr{c_hdfsfileinfo}, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))
    (C_NULL == info_ptr) && error(string("Error getting path ", path))

    ret = HdfsFileInfo(info_ptr)
    ccall((:hdfsFreeFileInfo, _libhdfs), Void, (Ptr{Void}, Int32), info_ptr, 1)
    ret
end

hdfs_is_directory(fs::HdfsFS, path::String) = (hdfs_get_path_info(fs,path).kind == HDFS_OBJ_DIR)

function hdfs_blocks(fs::HdfsFS, path::String, start::Integer, length::Integer)
    c_ptr = ccall((:hdfsGetHosts, _libhdfs), Ptr{Ptr{Ptr{Uint8}}}, (Ptr{Void}, Ptr{Uint8}, Int64, Int64), fs.ptr, bytestring(path), int64(start), int64(length))
    (C_NULL == c_ptr) && error("Error getting hosts for file $(path)")

    i = 1
    ret_vals = Array(Array{ASCIIString,1}, 0)
    while true
        h_list = unsafe_load(c_ptr, i)
        (h_list == C_NULL) && break
        
        j = 1
        arr = Array(ASCIIString, 0)
        while true
            hname = unsafe_load(h_list, j)
            (hname == C_NULL) && break
            push!(arr, bytestring(hname))
            #print(bytestring(hname), ", ")
            j += 1
        end
        push!(ret_vals, arr)
        #println("")
        i += 1
    end

    ccall((:hdfsFreeHosts, _libhdfs), Void, (Ptr{Ptr{Ptr{Uint8}}},), c_ptr)
    ret_vals
end


hdfs_get_default_block_size(fs::HdfsFS) = ccall((:hdfsGetDefaultBlockSize, _libhdfs), Int64, (Ptr{Void},), fs.ptr)

hdfs_get_capacity(fs::HdfsFS) = ccall((:hdfsGetCapacity, _libhdfs), Int64, (Ptr{Void},), fs.ptr)

hdfs_get_used(fs::HdfsFS) = ccall((:hdfsGetUsed, _libhdfs), Int64, (Ptr{Void},), fs.ptr)

hdfs_chown(fs::HdfsFS, path::String, owner::String, group::String) = ccall((:hdfsChown, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Uint8}, Ptr{Uint8}), fs.ptr, bytestring(path), bytestring(owner), bytestring(group))

hdfs_chmod(fs::HdfsFS, path::String, mode::Int16) = ccall((:hdfsChmod, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Int16), fs.ptr, bytestring(path), mode)

hdfs_utime(fs::HdfsFS, path::String, mtime::Integer, atime::Integer) = ccall((:hdfsUtime, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, TimeT, TimeT), fs.ptr, path, convert(TimeT, mtime), convert(TimeT, atime))


##
# File system implementation for HdfsFS
pwd(fs::HdfsFS) = urlparse(hdfs_pwd(fs)).url
function cd(fs::HdfsFS, path::String)
    hdfs_exists(fs, path) && (0 == hdfs_cd(fs, path)) && (return nothing)
    error("no such file or directory $path")
end
mkdir(fs::HdfsFS, path::String) = (0 == hdfs_mkdir(fs, path)) ? nothing : error("error creating directory $path")
mv(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String) = (srcFS == dstFS) ? mv(srcFs, src, dst) : (0 == hdfs_move(srcFS, src, dstFS, dst)) ? nothing : error("error moving file")
mv(fs::HdfsFS, src::String, dst::String) = (0 == hdfs_rename(fs, src, dst)) ? nothing : error("error renaming file")
rm(fs::HdfsFS, path::String) = (0 == hdfs_delete(fs, path)) ? nothing : error("error deleting $path")
rmdir(fs::HdfsFS, path::String) = rm(fs, path)
function readdir(fs::HdfsFS) 
    offset = length(pwd(fs))+2
    String[urlparse(fi.name).url[offset:] for fi in hdfs_list_directory(fs, ".")]
end
function readdir(fs::HdfsFS, path::String)
    (path == ".") && (return readdir(fs))
    p = pwd(fs)
    cd(fs, path)
    ret = readdir(fs)
    cd(fs, p)
    ret
end


##
# IO implementation for HdfsFile
# this is crap. should implement the array interface also
open(fs::HdfsFS, path::String) = open(fs, path, "r")
open(fs::HdfsFS, path::String, mode::String) = hdfs_open(fs, path, mode)
close(f::HdfsFile) = hdfs_close(f)
eof(f::HdfsFile) = (position(f) == filesize(f))

read(f::HdfsFile, x::Type{Uint8}) = (hdfs_read(f, 1)[1])[1]
function read{T}(f::HdfsFile, a::Array{T})
    remaining = length(a)
    while(remaining > 0)
        ret = hdfs_read(f, pointer(a, length(a)-remaining), remaining)
        (-1 == ret) && error("end of file")
        remaining -= ret
    end
    a
end
readbytes(f::HdfsFile, nb::Integer) = bytestring(read(f, Array(Uint8, nb)))
readall(f::HdfsFile) = readbytes(f, nb_available(f))

function peek(f::HdfsFile)
    ret = read(f, Uint8)
    skip(f, -1)
    ret
end

write(f::HdfsFile, p::Ptr, nb::Integer) = write(f, p, int(nb))
write(f::HdfsFile, p::Ptr, nb::Int) = hdfs_write(f, p, nb)
write(f::HdfsFile, x::Uint8) = write(f, Uint8[x])
write{T}(f::HdfsFile, a::Array{T}, len) = write_sub(f, a, 1, length(a))
write{T}(f::HdfsFile, a::Array{T}) = write(f, a, length(a))
write_sub{T}(f::HdfsFile, a::Array{T}, offs, len) = isbits(T) ? write(f, pointer(a,offs), len*sizeof(T)) : error("$T is not bits type")

flush(f::HdfsFile) = (hdfs_flush(f); nothing)
nb_available(f::HdfsFile) = hdfs_available(f)
function position(f::HdfsFile)
    p = hdfs_tell(f)
    (p >= 0) ? p : error("error getting current position")
end

stat(f::HdfsFile) = stat(f.fs, f.path)
stat(fs::HdfsFS, path::String) = hdfs_get_path_info(fs, path)

filesize(f::HdfsFile) = filesize(f.fs, f.path)
filesize(fs::HdfsFS, path::String) = (stat(fs,path)).size

seek(f::HdfsFile, n::Integer) = (0 == hdfs_seek(f, n))
seekend(f::HdfsFile) = seek(f, filesize(f))
seekstart(f::HdfsFile) = seek(f, 0)
skip(f::HdfsFile, n::Integer) = seek(f, n+position(f))

