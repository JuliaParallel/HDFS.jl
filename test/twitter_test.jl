##
# sample test program to produce monthly aggregate count of smileys
# used in tweets between year 2006 to 2009, based on data from infochimps.

using HDFS
using ChainedVectors

const smil = convert(Array{Uint8,1}, "smiley")

smiley_bchk(jc::HdfsJobCtx) = beginswithat(jc.info.rdr.cv, int(jc.info.next_rec_pos), smil) 
find_rec(jc::HdfsJobCtx) = hdfs_find_rec_csv(jc, '\n', '\t', 1024, smiley_bchk)

map_rec(jc::HdfsJobCtx) = return

# TODO: define standard collectors
function collect_rec(jc::HdfsJobCtx)
    ji = jc.info
    rec = ji.rec
    (length(rec) == 0) && return 

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    cnt = int(rec[3])
    smiley = rec[4]

    local monthly::Vector{Int}
    try
        monthly = getindex(ji.results, smiley)
    catch
        (nothing == ji.results) && (ji.results = Dict{String, Vector{Int}}())
        # represent 5 years worth of monthly data
        monthly = zeros(Int, 12*5)
        ji.results[smiley] = monthly
    end

    monthly[month_idx] += cnt
end

function reduce(reduced, results...)
    (nothing == reduced) && (reduced = Dict{String, Vector{Int}}())
    
    for d in results
        (nothing == d) && continue
        for (smiley,monthly) in d
            if(haskey(reduced, smiley))
                reduced[smiley] += monthly
            else
                reduced[smiley] = monthly
            end
        end
    end
    reduced
end

