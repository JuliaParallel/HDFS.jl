
const HDFS_OBJ_FILE = 'F'
const HDFS_OBJ_DIR = 'D'
const HDFS_OBJ_INVALID = ' '

# the following may not be entirely true. some 32 bits systems may have 64 bit time_t
if(64 == Base.WORD_SIZE)
    typealias TimeT Int64
else
    typealias TimeT Int32
end


## used to enforce types ##
type HdfsFS
    host::AbstractString
    port::Int
    user::AbstractString
    ptr::Ptr{Void}
    function HdfsFS(host::AbstractString, port::Int, user::AbstractString, pt::Ptr{Void})
        fs = new(host, port, user, pt)
        (pt != C_NULL) && finalizer(fs, finalize_hdfs_fs)
        fs
    end
end

type HdfsFile <: IO
    fs::HdfsFS
    path::AbstractString
    ptr::Ptr{Void}
    function HdfsFile(fs::HdfsFS, path::AbstractString, pt::Ptr{Void})
        f = new(fs, path, pt)
        (pt != C_NULL) && finalizer(f, hdfs_close)
        f
    end
end

type HdfsURL
    url::AbstractString
    function HdfsURL(url::AbstractString)
        uri = URI(url)
        (uri.schema != "hdfs") && error("not a HDFS URL")
        new(url)
    end
end

immutable c_hdfsfileinfo
    mKind::Cint             # file or directory. hoping the enum is int type (it is actually compiler dependent)
    mName::Ptr{Uint8}       # file name
    mLastMod::TimeT         # the last modification time for the file in seconds
    mSize::Int64            # the size of the file in bytes
    mReplication::Cshort    # the count of replicas
    mBlockSize::Int64       # the block size for the file
    mOwner::Ptr{Uint8}      # the owner of the file
    mGroup::Ptr{Uint8}      # the group associated with the file
    mPermissions::Cshort    # permissions associated with the file
    mLastAccess::TimeT      # the last access time for the file in seconds
end

type HdfsFileInfo
    kind::Int8
    name::AbstractString
    last_mod::Int64
    size::Int64
    replications::Int16
    block_sz::Int64
    owner::AbstractString
    grp::AbstractString
    permissions::Int16
    last_access::Int64

    HdfsFileInfo(pt::Ptr{c_hdfsfileinfo}) = (C_NULL != pt) ? HdfsFileInfo(unsafe_load(pt)) : new(HDFS_OBJ_INVALID, "", 0, 0, 0, 0, "", "", 0, 0)
    HdfsFileInfo(cfi::c_hdfsfileinfo) = 
        new(cfi.mKind, bytestring(cfi.mName), 
                        convert(Int64,cfi.mLastMod), 
                        cfi.mSize, cfi.mReplication, cfi.mBlockSize,
                        bytestring(cfi.mOwner), bytestring(cfi.mGroup), cfi.mPermissions, 
                        convert(Int64,cfi.mLastAccess))
end

