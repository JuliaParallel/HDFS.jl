using HDFS

host = isempty(ARGS) ? "localhost" : ARGS[1]
port = isempty(ARGS) ? 9000 : int(ARGS[2])

# connect 
fs = hdfs_connect(host, port)
println("connected to hdfs:://$(host):$(port)")

# print capacity, used
capacity = hdfs_get_capacity(fs)
used = hdfs_get_used(fs)
println("$(used) used from total $(capacity) bytes")
@assert 0 < used < capacity

blk_sz = hdfs_get_default_block_size(fs)
println("default block sz: $(blk_sz)")
@assert blk_sz > 0

# get current working directory
pwd = hdfs_pwd(fs)
println("current directory: $(pwd)")
@assert length(pwd) > 0

# make directory 
const TEST_DIR = "test_dir"
const TEST_FILE = "test_file"
hdfs_exists(fs, TEST_DIR) && hdfs_delete(fs, TEST_DIR)
@assert 0 == hdfs_mkdir(fs, TEST_DIR)
println("created new directory $(TEST_DIR)")
@assert true == hdfs_is_directory(fs, TEST_DIR)
@assert 0 == hdfs_cd(fs, TEST_DIR)
pwd1 = hdfs_pwd(fs)
println("current directory: $(pwd1)")
@assert pwd1 == string(pwd, "/", TEST_DIR)

@assert false == hdfs_exists(fs, TEST_FILE)                         # try exists on a file
fi = hdfs_open(fs, TEST_FILE, "w")                                  # create the file
println("created new file $(TEST_FILE)")
@assert 600 == hdfs_write(fi, repeat("hello ", 100))                # write to the file
@assert 0 == hdfs_flush(fi)                                         # flush writes
@assert 0 == hdfs_close(fi)                                         # close the file
println("wrote 600 bytes to file")
@assert false == hdfs_is_directory(fs, TEST_FILE)

@assert true == hdfs_exists(fs, TEST_FILE)                          # try exists on a file
fi = hdfs_open(fs, TEST_FILE, "r")                                  # read from the file
avlb = hdfs_available(fi)
println("bytes available: $(avlb)")
finfo = hdfs_get_path_info(fs, TEST_FILE)
println("file size bytes: $(finfo.size)")
@assert 600 == finfo.size
@assert 0 <= avlb <= finfo.size

println("reading...")
(buff, bytes) = hdfs_read(fi, 5)
readstr = bytestring(buff[1:bytes])
@assert "hello" == readstr

@assert 5 == hdfs_tell(fi)
@assert 0 == hdfs_seek(fi, 2)

(buff, bytes) = hdfs_read(fi, 5)
readstr = bytestring(buff[1:bytes])
@assert "llo h" == readstr

# test read at end of file
@assert 0 == hdfs_seek(fi, finfo.size-10)
(buff, bytes) = hdfs_read(fi, 50)
@assert 50 == length(buff)
@assert 10 == bytes

println("closing...")
@assert 0 == hdfs_close(fi)

blocks = hdfs_blocks(fs, TEST_FILE, 0, finfo.size)
@assert length(blocks) == 1

const NEW_TEST_FILE = string(TEST_FILE, ".new")
const NEW_TEST_FILE_2 = string(TEST_FILE, ".new2")
@assert 0 == hdfs_copy(fs, TEST_FILE, fs, NEW_TEST_FILE)
@assert true == hdfs_exists(fs, NEW_TEST_FILE)
@assert 0 == hdfs_move(fs, NEW_TEST_FILE, fs, NEW_TEST_FILE_2)
@assert true == hdfs_exists(fs, NEW_TEST_FILE_2)
@assert false == hdfs_exists(fs, NEW_TEST_FILE)

println("listing the directory...")
entries = hdfs_list_directory(fs, ".")
@assert 2 == length(entries)
for finfo in entries
    println("\t$(finfo.name)")
end

println("cleaning up...")
@assert 0 == hdfs_cd(fs, "..")
@assert 0 == hdfs_delete(fs, TEST_DIR)

println("passed")
