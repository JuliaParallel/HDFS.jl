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
    global jc = HdfsJobCtx{Vector{String}}(HDFS_HOST, HDFS_PORT, HDFS_DATA_FILE)
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
        

const smil = convert(Array{Uint8,1}, "smiley")
const REC_SEP = '\n'
const COL_SEP = "\t"
const MAX_REC_BYTES = 1024
 
function find_rec(jc::HdfsJobCtx{Vector{String}}, read_beyond::Bool = true)
    rdr = jc.rdr
    is_begin = (rdr.begin_blk == 1) # if first block, we should not ignore the first line
    start_pos = jc.next_rec_pos
    final_pos = start_pos + length(rdr.cv) - 1
    end_pos = 0

    if(!is_begin)
        end_pos = search(rdr.cv, REC_SEP, start_pos)
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, MAX_REC_BYTES)
            return find_rec(rdr, false)
        else
            start_pos = end_pos
        end
    end
  
    while(start_pos <= final_pos)
        end_pos = search(rdr.cv, REC_SEP, start_pos)-1
        #println("start_pos: $start_pos, final_pos: $final_pos, end_pos: $end_pos, read_beyond: $read_beyond") 
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, MAX_REC_BYTES)
            return find_rec(rdr, false)
        else
            # if no rec boundary found, assume all data in buffer is the record.
            # this is valid only if this is the end of the file.
            # which should be true if MAX_REC_BYTES is correct.
            # TODO: put a check and return error if not
            (0 >= end_pos) && (end_pos = final_pos)
            # TODO: optimize by implementing beginswithat in ChainedVector
            recbytes = rdr.cv[start_pos:end_pos]
            if(beginswithat(recbytes, 1, smil))
                rec = ascii(recbytes)
                jc.next_rec_pos = end_pos+2
                jc.rec = split(rec, COL_SEP)
                return :ok
            else
                start_pos = end_pos+2
            end
        end
    end
    jc.next_rec_pos = final_pos
    jc.rec = []
    :not_ok
end

function process_rec(rec::Vector{String})
    (length(rec) == 0) && return

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    cnt = int(rec[3])
    smiley = rec[4]

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

