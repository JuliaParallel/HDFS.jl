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
pwd1 = pwd(fs)
println("current directory: $(pwd1)")
@assert !isempty(pwd1)

# make directory 
const TEST_DIR = "test_dir"
const TEST_FILE = "test_file"
hdfs_exists(fs, TEST_DIR) && hdfs_delete(fs, TEST_DIR)
mkdir(fs, TEST_DIR)
println("created new directory $(TEST_DIR)")
@assert true == isdir(fs, TEST_DIR)
cd(fs, TEST_DIR)
pwd2 = pwd(fs)
println("current directory: $(pwd2)")
@assert pwd2 == string(pwd1, "/", TEST_DIR)

@assert false == hdfs_exists(fs, TEST_FILE)                         # try exists on a file
fi = open(fs, TEST_FILE, "w")                                  # create the file
println("created new file $(TEST_FILE)")
@assert 600 == write(fi, repeat("hello ", 100))                # write to the file
flush(fi)                                                      # flush writes
@assert 0 == close(fi)                                         # close the file
println("wrote 600 bytes to file")
@assert false == isdir(fs, TEST_FILE)

@assert true == hdfs_exists(fs, TEST_FILE)                          # try exists on a file
fi = open(fs, TEST_FILE, "r")                                  # read from the file
avlb = nb_available(fi)
println("bytes available: $(avlb)")
finfo = stat(fs, TEST_FILE)
println("file size bytes: $(finfo.size)")
@assert 600 == finfo.size
@assert 0 <= avlb <= finfo.size

println("reading...")
buff = read(fi, Array(Uint8, 5))
readstr = bytestring(buff[1:5])
@assert "hello" == readstr

@assert 5 == position(fi)
seek(fi, 2)

buff = read(fi, Array(Uint8, 5))
readstr = bytestring(buff[1:5])
@assert "llo h" == readstr

# test read at end of file
seek(fi, finfo.size-10)
(buff,bytes) = hdfs_read(fi, 50)
@assert 10 == bytes

println("closing...")
@assert 0 == close(fi)

blocks = hdfs_blocks(fs, TEST_FILE, 0, finfo.size)
@assert length(blocks) == 1

const NEW_TEST_FILE = string(TEST_FILE, ".new")
const NEW_TEST_FILE_2 = string(TEST_FILE, ".new2")
cp(fs, TEST_FILE, fs, NEW_TEST_FILE)
@assert true == hdfs_exists(fs, NEW_TEST_FILE)
mv(fs, NEW_TEST_FILE, fs, NEW_TEST_FILE_2)
@assert true == hdfs_exists(fs, NEW_TEST_FILE_2)
@assert false == hdfs_exists(fs, NEW_TEST_FILE)

println("listing the directory...")
entries = readdir(fs, ".")
@assert 2 == length(entries)
for fname in entries
    println("\t$(fname)")
end

println("cleaning up...")
cd(fs, "..")
rm(fs, TEST_DIR)

println("passed")
