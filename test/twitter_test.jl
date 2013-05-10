##
# sample test program to produce monthly aggregate count of smileys
# used in tweets between year 2006 to 2009, based on data from infochimps.

using HDFS

find_smiley(jr::HdfsReaderIter, next_rec_pos) = hdfs_find_rec_csv(jr, next_rec_pos, '\n', '\t', 1024, ("smiley", nothing, nothing, nothing))


##
# for finding total counts across all years
function map_total(rec)
    (length(rec) == 0) && return rec
    (rec[4], int(rec[3]))
end

# TODO: define standard collectors
function collect_total(results, rec)
    (length(rec) == 0) && return results
    smiley, cnt = rec

    try
        results[smiley] += cnt
    catch
        (nothing == results) && (results = Dict{String, Int}())
        results[smiley] = cnt
    end
    results
end

function reduce_total(reduced, results...)
    (nothing == reduced) && (reduced = Dict{String, Int}())
    
    for d in results
        (nothing == d) && continue
        for (smiley,cnt) in d
            haskey(reduced, smiley) ? (reduced[smiley] += cnt) : (reduced[smiley] = cnt)
        end
    end
    reduced
end


##
# for finding annual counts
function map_yearly(rec)
    (length(rec) == 0) && return rec

    ts = rec[2]
    ts_year = int(ts[1:4])
    (rec[4], (ts_year-2006+1), int(rec[3]))
end

# TODO: define standard collectors
function collect_yearly(results, rec)
    (length(rec) == 0) && return results
    smiley, year_idx, cnt = rec

    local yearly::Vector{Int}
    try
        yearly = getindex(results, smiley)
    catch
        (nothing == results) && (results = Dict{String, Vector{Int}}())
        yearly = zeros(Int, 5)
        results[smiley] = yearly
    end

    yearly[year_idx] += cnt
    results
end

function reduce_yearly(reduced, results...)
    (nothing == reduced) && (reduced = Dict{String, Vector{Int}}())
    
    for d in results
        (nothing == d) && continue
        for (smiley,yearly) in d
            haskey(reduced, smiley) ? (reduced[smiley] += yearly) : (reduced[smiley] = yearly)
        end
    end
    reduced
end


##
# for finding monthly counts
function map_monthly(rec)
    (length(rec) == 0) && return rec

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    (rec[4], month_idx, int(rec[3]))
end

# TODO: define standard collectors
function collect_monthly(results, rec)
    (length(rec) == 0) && return results
    smiley, month_idx, cnt = rec

    local monthly::Vector{Int}
    try
        monthly = getindex(results, smiley)
    catch
        (nothing == results) && (results = Dict{String, Vector{Int}}())
        # represent 5 years worth of monthly data
        monthly = zeros(Int, 12*5)
        results[smiley] = monthly
    end

    monthly[month_idx] += cnt
    results
end

function reduce_monthly(reduced, results...)
    (nothing == reduced) && (reduced = Dict{String, Vector{Int}}())
    
    for d in results
        (nothing == d) && continue
        for (smiley,monthly) in d
            haskey(reduced, smiley) ? (reduced[smiley] += monthly) : (reduced[smiley] = monthly)
        end
    end
    reduced
end

