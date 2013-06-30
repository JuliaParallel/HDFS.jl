## HDFS API

Available in module `HDFS`, these are wrappers over HDFS C APIs in `libhdfs`.
Files `test/dfs_test1.jl` and `test/dfs_test2.jl` have examples that illustrate API usage.

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
Returned from a call to `open` or `hdfs_open`, it holds a handle to the file, apart from the file system handle and the file URL. It needs to be passed around in future API calls involving the file.
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
Open a connection to a HDFS cluster. Returns a `HdfsFS`.
````
hdfs_connect(host::String="default", port::Integer=0, user::String="")
````

#### Creating/Deleting Files
````
mkdir(fs::HdfsFS, path::String) 

mv(fs::HdfsFS, src::String, dst::String)

# replication and block_sz are relevant only while creating a new file
open(fs::HdfsFS, path::String, mode::String="r", buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)
open(f::HdfsFile, mode::String="r", buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)
hdfs_open(url::String, mode::String, buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)

close(f::HdfsFile)

cp(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String)
cp(fs::HdfsFS, src::String, dst::String)

mv(srcFS::HdfsFS, src::String, dstFS::HdfsFS, dst::String) 

rm(fs::HdfsFS, path::String) 

hdfs_set_replication(fs::HdfsFS, path::String, replication::Integer)
````

#### Reading/Writing Files
`HDFSFile` provides an `IOStream` interface. Most `IO` APIs would work as usual on it.

#### Getting Information about File system and files
````
stat(f::HdfsFile)
stat(fs::HdfsFS, path::String)

isdir(fs::HdfsFS, path::String)

pwd(fs::HdfsFS)

cd(fs::HdfsFS, path::String)

readdir(fs::HdfsFS, path::String=".")

hdfs_get_default_block_size(fs::HdfsFS)
hdfs_get_capacity(fs::HdfsFS)
hdfs_get_used(fs::HdfsFS)

hdfs_exists(fs::HdfsFS, path::String)
hdfs_blocks(fs::HdfsFS, path::String, start::Integer, length::Integer)
````


