##
# sample test program to produce monthly aggregate count of smileys
# used in tweets between year 2006 to 2009, based on data from infochimps.

using HDFS
using ChainedVectors

type SmileyData
    monthly::Array{Int, 1}
    SmileyData() = new(zeros(Int, 12*5)) # represent 5 years of data in 12*5 monthly slots
end

const smil = convert(Array{Uint8,1}, "smiley")

smiley_bchk(jc::HdfsJobCtx) = beginswithat(jc.rdr.cv, int(jc.next_rec_pos), smil) 
find_rec(jc::HdfsJobCtx) = hdfs_find_rec_csv(jc, '\n', '\t', 1024, smiley_bchk)

function process_rec(jc::HdfsJobCtx)
    rec = jc.rec
    (length(rec) == 0) && return

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    cnt = int(rec[3])
    smiley = rec[4]

    local smrec::SmileyData
    try
        smrec = getindex(jc.results, smiley)
    catch
        (nothing == jc.results) && (jc.results = Dict{String, SmileyData}())
        smrec = SmileyData()
        jc.results[smiley] = smrec
    end

    smrec.monthly[month_idx] += cnt
end

function reduce(jc::HdfsJobCtx, results::Vector)
    reduced = Dict{String, Vector{Int}}()
    for d in results
        for (smiley,sd) in d
            monthly = sd.monthly

            if(has(reduced, smiley))
                da = reduced[smiley]
                da += monthly
            else
                reduced[smiley] = monthly
            end
        end
    end
    reduced
end

function summarize(results::Dict{String, Vector{Int}})
    for d in results
        smiley = d[1]
        monthly = d[2]
        println(smiley, " ==>> total: ", sum(monthly), " max/min: ", max(monthly), "/", min(monthly))
    end
end

