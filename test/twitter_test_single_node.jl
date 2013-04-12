using HDFS

function process_file(fs, file, finfo, hostinfo)
    blksz = finfo.block_sz
    sz = finfo.size

    start_pos = 1
    buff = Array(Uint8, blksz)

    for hinfo in hostinfo
        end_pos = start_pos + blksz - 1
        (end_pos > sz) && (end_pos = sz)

        len = end_pos - start_pos + 1
        println(hinfo[1], ": reading block at ", start_pos, " of ", sz, " len ", len)
        hdfs_read(fs, file, convert(Ptr{Void}, buff), len)
        #println("processing a new block")
        process_block(buff, len)
        start_pos = end_pos+1
    end
end

function process_block(buff, len)
    println("processing block of len $len")

    start_pos = 1
    blk_end_pos = start_pos + len
    while(start_pos < (len+1))
        rec, end_pos = find_record(buff, start_pos, len)
        try
            process_record(rec)
        catch
            println("possibly partial record in block. ignoring.")
        end
        start_pos = end_pos+1
    end 
end

function find_record(buff, start_pos, len)
    end_pos = search(buff, '\n', start_pos)-1
    (start_pos+len-1 < end_pos) && (end_pos = start_pos+len-1)
    (0 >= end_pos) && (end_pos = len)
    rec = ascii(buff[start_pos:end_pos])
    cols = split(rec, "\t")
    (cols, end_pos+1)
end


const BEGIN_YEAR = 2006

type SmileyData
    # represent 5 years of data in 12*5 monthly slots
    monthly::Array{Int, 1}
    SmileyData() = new(zeros(Int, 12*5))
end


sd = Dict{String, SmileyData}()
function process_record(rec)
    (rec[1] != "smiley") && return

    ts = rec[2]
    ts_year = ts[1:4]
    ts_mon = ts[5:6]
    smiley = rec[4]
    cnt = int(rec[3])

    local smrec::SmileyData

    try
        smrec = getindex(sd, smiley)
    catch
        smrec = SmileyData()
        sd[smiley] = smrec
    end

    month_idx = 12*(int(ts_year)-BEGIN_YEAR) + int(ts_mon)
    smrec.monthly[month_idx] += cnt
    #println(rec, length(rec))
end

function write_smiley_data_summary()
    for d in sd
        smiley = d[1]
        monthly = d[2].monthly
        println(smiley, " ==>> total: ", sum(monthly), " max/min: ", max(monthly), "/", min(monthly))
    end
end


host = ARGS[1]
port = int(ARGS[2])
file = ARGS[3]

fs = hdfs_connect(host, port)

finfo = hdfs_get_path_info(fs, file)
blksz = finfo.block_sz
sz = finfo.size

hostinfo = hdfs_get_hosts(fs, file, 0, sz)

file = hdfs_open_file_read(fs, file)

process_file(fs, file, finfo, hostinfo)

hdfs_close_file(fs, file)

println(sd)

write_smiley_data_summary()


 
