##
# sample test program to produce monthly aggregate count of smileys
# used in tweets between year 2006 to 2009, based on data from infochimps.

using HDFS

const HDFS_HOST = "localhost"
const HDFS_PORT = 9000
const HDFS_DATA_FILE = "/twitter_data.txt"

type SmileyData
    monthly::Array{Int, 1}
    SmileyData() = new(zeros(Int, 12*5)) # represent 5 years of data in 12*5 monthly slots
end

function init_job_ctx()
    global jc = HdfsJobCtx(HDFS_HOST, HDFS_PORT, HDFS_DATA_FILE)
    global sd = Dict{String, SmileyData}()
end

function get_job_ctx()
    global jc
    jc
end

function destroy_job_ctx()
    global jc
    finalize_hdfs_job_ctx(jc)
end

function gather_results()
    global sd
    sd_ret = sd
    sd = Dict{String, SmileyData}()
    sd_ret
end

beginswithat(a::Array{Uint8,1}, pos::Integer,  b::Array{Uint8,1}) = ((length(a)-pos+1) >= length(b) && ccall(:strncmp, Int32, (Ptr{Uint8}, Ptr{Uint8}, Uint), pointer(a)+pos-1, b, length(b)) == 0)


const smil::Array{Uint8,1} = convert(Array{Uint8,1}, "smiley")
function find_record(buff::Array{Uint8,1}, start_pos::Int64, len::Int64)
    local final_pos::Int64 = start_pos+len-1;
    while(start_pos <= final_pos)
        local end_pos::Int = search(buff, '\n', convert(Int, start_pos))-1
        (0 >= end_pos) && (end_pos = final_pos)
        if(beginswithat(buff, start_pos, smil))
            rec = ascii(buff[start_pos:end_pos])
            cols = split(rec, "\t")
            return (cols, int64(end_pos)+2)
        else 
            start_pos = int64(end_pos)+2
        end
    end
    ([], final_pos)
end


function process_record(rec)
    (length(rec) == 0) && return

    local ts::String = rec[2]
    local ts_year::Int = int(ts[1:4])
    local ts_mon::Int = int(ts[5:6])
    local month_idx::Int = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    local cnt::Int = int(rec[3])
    local smiley::String = rec[4]

    local smrec::SmileyData

    try
        smrec = getindex(sd, smiley)
    catch
        smrec = SmileyData()
        sd[smiley] = smrec
    end

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

