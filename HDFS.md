## HDFS API

These are mostly wrappers over HDFS C APIs in `libhdfs`.

### Types

#### HdfsFS
Holds a handle to the file system along with the connection specification (host, port and username). It can be obtained from a call to `hdfs_connect` and needs to be passed around in future API calls.
````
type HdfsFS
    host::String 
    port::Int
    user::String
    ptr::Ptr{Void}
    ...
end
````

#### HdfsFile
Returned from a call to `hdfs_open_file`, it holds a handle to the file. It needs to be passed around in future API calls involving the file.
````
type HdfsFile
  ptr::Ptr{Void}
end
````

#### HdfsFileInfo
Holds information about a HDFS file. Returned from a call to `hdfs_get_path_info`.
````
type HdfsFileInfo
    kind::Int8              # one of HDFS_OBJ_FILE, HDFS_OBJ_DIR, HDFS_OBJ_INVALID
    name::String            # full hdfs URL to the file
    last_mod::Int64
    size::Int64             # size in bytes
    replications::Int16     # number of replications
    block_sz::Int64         # block size in bytes
    owner::String
    grp::String
    permissions::Int16
    last_access::Int64
    ...
end
````

#### HdfsFileInfoList
Holds a list of `HdfsFileInfo`. This is returned from a call to `hdfs_list_directory`.
````
type HdfsFileInfoList
    arr::Array{HdfsFileInfo, 1}
    ...
end
````

### APIs

#### Connecting to HDFS
Open a connection to a HDFS cluster. Returns a HdfsFS.
On failure the `ptr` member of returned value would be `NULL`.
````
hdfs_connect(host::String="default", port::Integer=0, user::String="")
````

#### Creating/Deleting Files
````
hdfs_create_directory(fs::HdfsFS, path::String)
hdfs_rename(fs::HdfsFS, old_path::String, new_path::String)

hdfs_open_file(fs::HdfsFS, path::String, flags::Integer, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)
hdfs_open_file_read(fs::HdfsFS, path::String)
hdfs_open_file_write(fs::HdfsFS, path::String)
hdfs_open_file_append(fs::HdfsFS, path::String)

hdfs_close_file(fs::HdfsFS, file::HdfsFile)

hdfs_copy(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String)
hdfs_move(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String)
hdfs_delete(fs::HdfsFS, path::String)

hdfs_set_replication(fs::HdfsFS, path::String, replication::Integer)
````

#### Reading/Writing Files
````
hdfs_read(fs::HdfsFS, file::HdfsFile, len::Integer)
hdfs_read(fs::HdfsFS, file::HdfsFile, buff::Ptr{Void}, len::Integer)
hdfs_pread(fs::HdfsFS, file::HdfsFile, position::Int64, buff::Ptr{Void}, len::Integer)

hdfs_write(fs::HdfsFS, file::HdfsFile, buff::ASCIIString)
hdfs_write(fs::HdfsFS, file::HdfsFile, buff::ASCIIString, len::Integer)
hdfs_write(fs::HdfsFS, file::HdfsFile, buff::Ptr{Void}, len::Integer)

hdfs_seek(fs::HdfsFS, file::HdfsFile, desired_pos) 
hdfs_tell(fs::HdfsFS, file::HdfsFile) 
hdfs_flush(fs::HdfsFS, file::HdfsFile)
````

#### Getting Information about File system and files
````
hdfs_get_default_block_size(fs::HdfsFS)
hdfs_get_capacity(fs::HdfsFS)
hdfs_get_used(fs::HdfsFS)

hdfs_get_path_info(fs::HdfsFS, path::String)
hdfs_is_directory(fs::HdfsFS, path::String)
hdfs_exists(fs::HdfsFS, path::String)
hdfs_available(fs::HdfsFS, file::HdfsFile)

hdfs_get_working_directory(fs::HdfsFS, buff_sz::Integer)
hdfs_set_working_directory(fs::HdfsFS, path::String)

hdfs_list_directory(fs::HdfsFS, path::String)
hdfs_get_hosts(fs::HdfsFS, path::String, start::Integer, length::Integer)
````

### TODO
- simplify and make API similar to IO 

