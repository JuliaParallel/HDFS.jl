const _libhdfs = "libhdfs"

const HDFS_OBJ_FILE = 'F'
const HDFS_OBJ_DIR = 'D'
const HDFS_OBJ_INVALID = ' '

const O_WRONLY = 1
const O_RDONLY = 0
const O_APPEND = 8

## used to enforce typing ##

type HdfsFS
    ptr::Ptr{Void}
    function HdfsFS(pt::Ptr{Void})
        fs = new(pt)
        if(fs != C_NULL)
            finalizer(fs, finalize_hdfs_fs)
        end
        fs
    end
end

type HdfsFile
  ptr::Ptr{Void}
end

immutable c_hdfsfileinfo
    mKind::Cint             # file or directory. hoping the enum is int type (it is actually compiler dependent)
    mName::Ptr{Uint8}       # file name
    mLastMod::Int64         # the last modification time for the file in seconds. hoping time_t is 64 bits
    mSize::Int64            # the size of the file in bytes
    mReplication::Cshort    # the count of replicas
    mBlockSize::Int64       # the block size for the file
    mOwner::Ptr{Uint8}      # the owner of the file
    mGroup::Ptr{Uint8}      # the group associated with the file
    mPermissions::Cshort    # permissions associated with the file
    mLastAccess::Int64      # the last access time for the file in seconds. hoping time_t is 64 bits
end

type HdfsFileInfo
    c_info_ptr::Ptr{c_hdfsfileinfo}
    kind::Int8
    name::String
    last_mod::Int64
    size::Int64
    replications::Int16
    block_sz::Int64
    owner::String
    grp::String
    permissions::Int16
    last_access::Int64

    function HdfsFileInfo(pt::Ptr{c_hdfsfileinfo})
        if(C_NULL != pt)
            cfi::c_hdfsfileinfo = unsafe_ref(pt)
            fi = new(pt, cfi.mKind, bytestring(cfi.mName), cfi.mLastMod, cfi.mSize, cfi.mReplication, cfi.mBlockSize, 
                        bytestring(cfi.mOwner), bytestring(cfi.mGroup), cfi.mPermissions, cfi.mLastAccess)
            finalizer(fi, finalize_file_info)
        else
            fi = new(C_NULL, HDFS_OBJ_INVALID, "", 0, 0, 0, 0, "", "", 0, 0)
        end
        fi
    end

    function HdfsFileInfo(cfi::c_hdfsfileinfo)
        new(C_NULL, cfi.mKind, bytestring(cfi.mName), cfi.mLastMod, cfi.mSize, cfi.mReplication, cfi.mBlockSize,
                        bytestring(cfi.mOwner), bytestring(cfi.mGroup), cfi.mPermissions, cfi.mLastAccess)
    end
end

type HdfsFileInfoList
    c_info_ptr::Ptr{c_hdfsfileinfo}
    arr::Array{HdfsFileInfo, 1}

    function HdfsFileInfoList(pt::Ptr{c_hdfsfileinfo}, len::Int32)
        if(C_NULL != pt)
            local carr::Array{c_hdfsfileinfo,1} = pointer_to_array(pt, (int64(len),))
            fiarr = [HdfsFileInfo(x) for x in carr]
            fi = new(pt, fiarr)
            finalizer(fi, finalize_file_info_list)
        else
            fi = new(C_NULL, Array(HdfsFileInfo, 0))
        end
        fi
    end
end

