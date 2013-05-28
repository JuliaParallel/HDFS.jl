## HDFS API

These are wrappers over HDFS C APIs in `libhdfs`.
File `test/dfs_test.jl` has examples that illustrate API usage.

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
Returned from a call to `hdfs_open`, it holds a handle to the file, apart from the file system handle and the file URL. It needs to be passed around in future API calls involving the file.
````
type HdfsFile
    fs::HdfsFS
    path::String
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

### APIs

#### Connecting to HDFS
Open a connection to a HDFS cluster. Returns a HdfsFS.
````
hdfs_connect(host::String="default", port::Integer=0, user::String="")
````

#### Creating/Deleting Files
````
hdfs_mkdir(fs::HdfsFS, path::String)
hdfs_rename(fs::HdfsFS, old_path::String, new_path::String)

hdfs_open(fs::HdfsFS, path::String, mode::String, buffer_sz::Integer=0, replication::Integer=0, bsz::Integer=0)

hdfs_close(file::HdfsFile)

hdfs_copy(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String)
hdfs_move(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String)
hdfs_delete(fs::HdfsFS, path::String)

hdfs_set_replication(fs::HdfsFS, path::String, replication::Integer)
````

#### Reading/Writing Files
````
hdfs_read(file::HdfsFile, len::Integer)
hdfs_read(file::HdfsFile, buff::Ptr{Void}, len::Integer)
hdfs_pread(file::HdfsFile, position::Int64, buff::Ptr{Void}, len::Integer)

hdfs_write(file::HdfsFile, buff::ASCIIString)
hdfs_write(file::HdfsFile, buff::ASCIIString, len::Integer)
hdfs_write(file::HdfsFile, buff::Ptr{Void}, len::Integer)

hdfs_seek(file::HdfsFile, desired_pos) 
hdfs_tell(file::HdfsFile) 
hdfs_flush(file::HdfsFile)
hdfs_available(file::HdfsFile)
````

#### Getting Information about File system and files
````
hdfs_get_default_block_size(fs::HdfsFS)
hdfs_get_capacity(fs::HdfsFS)
hdfs_get_used(fs::HdfsFS)

hdfs_get_path_info(fs::HdfsFS, path::String)
hdfs_is_directory(fs::HdfsFS, path::String)
hdfs_exists(fs::HdfsFS, path::String)

hdfs_pwd(fs::HdfsFS, buff::Ptr{Uint8}, buff_sz::Integer) 
hdfs_pwd(fs::HdfsFS, buff_sz::Integer)
hdfs_pwd(fs::HdfsFS)
hdfs_cd(fs::HdfsFS, path::String)

hdfs_list_directory(fs::HdfsFS, path::String)
hdfs_blocks(fs::HdfsFS, path::String, start::Integer, length::Integer)
````

### TODO
- simplify and make API similar to IO 

