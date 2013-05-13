##
# sample test program to produce monthly aggregate count of smileys
# used in tweets between year 2006 to 2009, based on data from infochimps.

using HDFS


##
# find smiley records from HDFS CSV file
find_smiley(jr::HdfsReaderIter, next_rec_pos) = hdfs_find_rec_csv(jr, next_rec_pos, '\n', '\t', 1024, ("smiley", nothing, nothing, nothing))


##
# for finding total counts across all years
function map_total(rec)
    ((nothing == rec) || (length(rec) == 0)) && return []
    [(rec[4], int(rec[3]))]
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
    ((nothing == rec) || (length(rec) == 0)) && return []

    ts = rec[2]
    ts_year = int(ts[1:4])
    [(rec[4], (ts_year-2006+1), int(rec[3]))]
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

find_yearly(jr::MapResultReaderIter, iter_status) = mr_result_find_rec(jr, iter_status)
map_total_from_yearly(rec) = [(rec[1], sum(rec[2]))]


##
# for finding monthly counts
function map_monthly(rec)
    ((nothing == rec) || (length(rec) == 0)) && return []

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    [(rec[4], month_idx, int(rec[3]))]
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

find_monthly(jr::MapResultReaderIter, iter_status) = mr_result_find_rec(jr, iter_status)
function map_yearly_from_monthly(rec) 
    b = rec[2]
    ret = Array(Tuple,0)
    for i in 1:(length(b)/12)
        slice_end = 12*i
        push!(ret, (rec[1], i, sum(b[1+slice_end-12:slice_end])))
    end
    ret
end

 

 
