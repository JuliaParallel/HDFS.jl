module HDFS

export  hdfs_connect, hdfs_connect_as_user,
        hdfs_exists, hdfs_delete, 
        hdfs_flush, hdfs_get_capacity, hdfs_get_default_block_size, hdfs_get_path_info, hdfs_get_used, 
        hdfs_set_working_directory, hdfs_get_working_directory, hdfs_list_directory, hdfs_available,
        hdfs_create_directory, hdfs_rename, hdfs_close_file,
        hdfs_open_file, hdfs_open_file_read, hdfs_open_file_write, hdfs_open_file_append,
        hdfs_copy, hdfs_move,
        hdfs_pread, hdfs_read, hdfs_seek, hdfs_tell, hdfs_write,
        hdfs_set_replication,
        hdfs_get_hosts,
        HDFS_OBJ_FILE, HDFS_OBJ_DIR, HDFS_OBJ_INVALID

include("hdfs_types.jl")

finalize_file_info_list(fi::HdfsFileInfoList) = ccall((:hdfsFreeFileInfo, _libhdfs), Void, (Ptr{Void}, Int32), fi.c_info_ptr, length(fi.arr))
finalize_file_info(fi::HdfsFileInfo) = ccall((:hdfsFreeFileInfo, _libhdfs), Void, (Ptr{Void}, Int32), fi.c_info_ptr, 1)
finalize_hdfs_fs(fs::HdfsFS) = ccall((:hdfsDisconnect, _libhdfs), Int32, (Ptr{Void},),fs.ptr)


hdfs_connect_as_user(host::String, port::Integer, user::String) = HdfsFS(ccall((:hdfsConnectAsUser, _libhdfs), Ptr{Void}, (Ptr{Uint8}, Int32, Ptr{Uint8}), bytestring(host), int32(port), bytestring(user)))
hdfs_connect(host::String="default", port::Integer=0) = HdfsFS(ccall((:hdfsConnect, _libhdfs), Ptr{Void}, (Ptr{Uint8}, Int32), bytestring(host), int32(port)))


# file control flags need to be done manually
function hdfs_open_file(fs::HdfsFS, path::String, flags::Integer, buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)
  file = ccall((:hdfsOpenFile, _libhdfs), Ptr{Void}, (Ptr{Void}, Ptr{Uint8}, Int32, Int32, Int16, Int32), fs.ptr, bytestring(path), int32(flags), int32(buffer_sz), int16(replication), int32(block_sz))
  return HdfsFile(file)
end
hdfs_open_file_read(fs::HdfsFS, path::String) = hdfs_open_file(fs, path, Base.JL_O_RDONLY)
hdfs_open_file_write(fs::HdfsFS, path::String) = hdfs_open_file(fs, path, Base.JL_O_WRONLY)
hdfs_open_file_append(fs::HdfsFS, path::String) = hdfs_open_file(fs, path, Base.JL_O_WRONLY | Base.JL_O_APPEND)

hdfs_close_file(fs::HdfsFS, file::HdfsFile) = ccall((:hdfsCloseFile, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}), fs.ptr, file.ptr)

hdfs_exists(fs::HdfsFS, path::String) = ccall((:hdfsExists, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_seek(fs::HdfsFS, file::HdfsFile, desired_pos) = ccall((:hdfsSeek, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Int64), fs.ptr, file.ptr, desired_pos)

hdfs_tell(fs::HdfsFS, file::HdfsFile) = ccall((:hdfsTell, _libhdfs), Int64, (Ptr{Void}, Ptr{Void}), fs.ptr, file.ptr)

hdfs_read(fs::HdfsFS, file::HdfsFile, buff::Ptr{Void}, len::Integer) = ccall((:hdfsRead, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Ptr{Void}, Int32), fs.ptr, file.ptr, buff, int32(len))
function hdfs_read(fs::HdfsFS, file::HdfsFile, len::Integer)
    local buff = Array(Uint8, len) 
    if(-1 == (r = hdfs_read(fs, file, convert(Ptr{Void}, buff), len)))
        error("error reading file: -1")
    end
    println("read ", r, " bytes")
    buff
end

hdfs_pread(fs::HdfsFS, file::HdfsFile, position::Int64, buff::Ptr{Void}, len::Integer) = ccall((:hdfsPread, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Int64, Ptr{Void}, Int32), fs.ptr, file.ptr, position, buff, len)
function hdfs_pread(fs::HdfsFS, file::HdfsFile, position, len)
    local buff = Array(Uint8, len) 
    if(-1 == (r = hdfs_pread(fs, file, position, buff, len)))
        error("error reading file: -1")
    end
    print("read ",r," bytes\n")
    buff
end

#can be passed an ASCIIString (length not necessary in that case)
hdfs_write(fs::HdfsFS, file::HdfsFile, buff::Ptr{Void}, len::Integer) = ccall((:hdfsWrite, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}, Ptr{Void}, Int32), fs.ptr, file.ptr, buff, int32(len))
hdfs_write(fs::HdfsFS, file::HdfsFile, buff::ASCIIString, len::Integer) = hdfs_write(fs, file, bytestring(buff), len)
hdfs_write(fs::HdfsFS, file::HdfsFile, buff::ASCIIString) = hdfs_write(fs, file, bytesteing(buff), length(buff))

hdfs_flush(fs::HdfsFS, file::HdfsFile) = ccall((:hdfsFlush, _libhdfs), Int32, (Ptr{Void}, Ptr{Void}), fs.ptr, file.ptr)

hdfs_available(fs::HdfsFS, file::HdfsFile) = ccall((:hdfsAvailable, _libhdfs), Int32, (Ptr{Void},Ptr{Void}), fs.ptr, file.ptr)

hdfs_copy(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String) = ccall((:hdfsCopy, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Void}, Ptr{Uint8}), srcFS.ptr, bytestring(src), dstFS.ptr, bytestring(dst))

hdfs_move(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String) = ccall((:hdfsMove, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Void}, Ptr{Uint8}), srcFS.ptr, bytestring(src), dstFS.ptr, bytestring(dst))

hdfs_delete(fs::HdfsFS, path::String) = ccall((:hdfsDelete, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_rename(fs::HdfsFS, old_path, new_path::String) = ccall((:hdfsRename, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Ptr{Uint8}), fs.ptr, bytestring(old_path), bytestring(new_path))

hdfs_get_working_directory(fs::HdfsFS, buff::Ptr{Uint8}, buff_sz::Integer)=ccall((:hdfsGetWorkingDirectory, _libhdfs), Ptr{Uint8}, (Ptr{Void}, Ptr{Uint8}, Int32), fs.ptr, buff, int32(buff_sz))
function hdfs_get_working_directory(fs::HdfsFS, buff_sz::Integer)
    buff = Array(Uint8, buff_sz)
    path = hdfs_get_working_directory(fs, buff, buff_sz)
    if(C_NULL == path)
        error("Error getting working directory")
    end
    return bytestring(path)
end
hdfs_get_working_directory(fs::HdfsFS) = hdfs_get_working_directory(fs, 1024)

hdfs_set_working_directory(fs::HdfsFS, path::String) = ccall((:hdfsSetWorkingDirectory, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_create_directory(fs::HdfsFS, path::String) = ccall((:hdfsCreateDirectory, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path))

hdfs_set_replication(fs::HdfsFS, path::String, replication::Integer)=ccall((:hdfsSetReplication, _libhdfs), Int32, (Ptr{Void}, Ptr{Uint8}, Int16), fs.ptr, bytestring(path), int16(replication))

function hdfs_list_directory(fs::HdfsFS, path::String)
    local num_entries::Array{Int32,1} = zeros(Int32, 1)
    file_info_list = ccall((:hdfsListDirectory, _libhdfs), Ptr{c_hdfsfileinfo}, (Ptr{Void}, Ptr{Uint8}, Ptr{Int32}), fs.ptr, bytestring(path), num_entries)
    if(C_NULL == file_info_list)
        error(string("Error listing path ", path))
    end
    HdfsFileInfoList(file_info_list, num_entries[1]) 
end

hdfs_get_path_info(fs::HdfsFS, path::String) = HdfsFileInfo(ccall((:hdfsGetPathInfo, _libhdfs), Ptr{c_hdfsfileinfo}, (Ptr{Void}, Ptr{Uint8}), fs.ptr, bytestring(path)))

function hdfs_get_hosts(fs::HdfsFS, path::String, start::Integer, length::Integer)
    local c_ptr::Ptr{Ptr{Ptr{Uint8}}} = ccall((:hdfsGetHosts, _libhdfs), Ptr{Ptr{Ptr{Uint8}}}, (Ptr{Void}, Ptr{Uint8}, Int64, Int64), fs.ptr, bytestring(path), int64(start), int64(length))
    if(C_NULL == c_ptr)
        error(string("Error getting hosts for file", path))
    end

    local i::Int = 1
    local ret_vals::Array{Array{String,1},1} = Array(Array{String,1}, 0)
    while true
        local h_list::Ptr{Ptr{Uint8}} = unsafe_ref(c_ptr, i)
        (h_list == C_NULL) && break
        
        local j::Int = 1
        local arr::Array{String,1} = Array(String, 0)
        while true
            local hname::Ptr{Uint8} = unsafe_ref(h_list, j)
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

end

