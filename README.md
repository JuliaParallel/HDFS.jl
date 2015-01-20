## HDFS.jl

HDFS.jl wraps the HDFS C library `libhdfs` and provides APIs similar to Julia Filesystem APIs which can be used for direct access to HDFS files.

### Types

#### HdfsFS
Holds a handle to the file system along with the connection specification (host, port and username). It can be obtained from a call to `hdfs_connect` and needs to be passed around in future API calls.

````
type HdfsFS
    host::AbstractString 
    port::Int
    user::AbstractString
    ptr::Ptr{Void}
    ...
end
````

#### HdfsFile

Returned from a call to `open` or `hdfs_open`, it holds a handle to the file, apart from the file system handle and the file URL. It needs to be passed around in future API calls involving the file.

````
type HdfsFile
    fs::HdfsFS
    path::AbstractString
    ptr::Ptr{Void}
end
````

#### HdfsFileInfo

Holds information about a HDFS file. Returned from a call to `hdfs_get_path_info`.

````
type HdfsFileInfo
    kind::Int8              # one of HDFS_OBJ_FILE, HDFS_OBJ_DIR, HDFS_OBJ_INVALID
    name::AbstractString    # full HDFS URL to the file
    last_mod::Int64
    size::Int64             # size in bytes
    replications::Int16     # number of replications
    block_sz::Int64         # block size in bytes
    owner::AbstractString
    grp::AbstractString
    permissions::Int16
    last_access::Int64
    ...
end
````

### APIs

#### Connecting to HDFS

Open a connection to a HDFS cluster. Returns a `HdfsFS`.
````
hdfs_connect(host::AbstractString="default", port::Integer=0, user::AbstractString="")
````

#### Creating/Deleting Files

````
mkdir(fs::HdfsFS, path::AbstractString) 

mv(fs::HdfsFS, src::AbstractString, dst::AbstractString)

# replication and block_sz are relevant only while creating a new file
open(fs::HdfsFS, path::AbstractString, mode::AbstractString="r", buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)
open(f::HdfsFile, mode::AbstractString="r", buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)
hdfs_open(url::AbstractString, mode::AbstractString, buffer_sz::Integer=0, replication::Integer=0, block_sz::Integer=0)

close(f::HdfsFile)

cp(srcFS::HdfsFS, src::AbstractString, dstFS::HdfsFS, dst::AbstractString)
cp(fs::HdfsFS, src::AbstractString, dst::AbstractString)

mv(srcFS::HdfsFS, src::AbstractString, dstFS::HdfsFS, dst::AbstractString) 

rm(fs::HdfsFS, path::AbstractString) 

hdfs_set_replication(fs::HdfsFS, path::AbstractString, replication::Integer)
````

#### Reading/Writing Files

`HDFSFile` provides an `IOStream` interface. Most `IO` APIs would work as usual on it.

#### Getting Information about File system and files

````
stat(f::HdfsFile)
stat(fs::HdfsFS, path::AbstractString)

isdir(fs::HdfsFS, path::AbstractString)

pwd(fs::HdfsFS)

cd(fs::HdfsFS, path::AbstractString)

readdir(fs::HdfsFS, path::AbstractString=".")

hdfs_get_default_block_size(fs::HdfsFS)
hdfs_get_capacity(fs::HdfsFS)
hdfs_get_used(fs::HdfsFS)

hdfs_exists(fs::HdfsFS, path::AbstractString)
hdfs_blocks(fs::HdfsFS, path::AbstractString, start::Integer, length::Integer)
````

Files `test/dfs_test1.jl` and `test/dfs_test2.jl` have examples that illustrate API usage.

Here are also some [installation instructions](INSTALL.md) in case you have trouble installing on your platform.

Note: HDFS.jl is based on libhdfs.jl, originally written by Benjamin Yang (@benyang) and part of the julia/extras.
