using HDFS
using Gaston

##
# find smiley records from HDFS CSV file
find_count_of_typ(jr::HdfsReaderIter, next_rec_pos, ttyp::String) = hdfs_find_rec_csv(jr, next_rec_pos, '\n', '\t', 1024, (ttyp, nothing, nothing, nothing))

function map_count_monthly(rec, tag::Regex)
    ((nothing == rec) || (length(rec) == 0) || !ismatch(tag, rec[4])) && return []

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    [(rec[4], month_idx, int(rec[3]))]
end

# TODO: define standard collectors
function collect_count_monthly(results, rec)
    (length(rec) == 0) && return results
    hashtag, month_idx, cnt = rec

    local monthly::Vector{Int}
    try
        monthly = getindex(results, hashtag)
    catch
        (nothing == results) && (results = Dict{String, Vector{Int}}())
        # represent 5 years worth of monthly data
        monthly = zeros(Int, 12*5)
        results[hashtag] = monthly
    end

    monthly[month_idx] += cnt
    results
end

function reduce_count_monthly(reduced, results...)
    (nothing == reduced) && (reduced = Dict{String, Vector{Int}}())
    
    for d in results
        (nothing == d) && continue
        for (hashtag,monthly) in d
            haskey(reduced, hashtag) ? (reduced[hashtag] += monthly) : (reduced[hashtag] = monthly)
        end
    end
    reduced
end

function do_plot_counts(furl::String, typ::String, tag::String)
    println("starting mapreduce...")
    j_mon = mapreduce(furl, (x,y)->find_count_of_typ(x,y,typ), x->map_count_monthly(x, Regex(tag)), collect_count_monthly, reduce_count_monthly)

    println("waiting for mapreduce to finish...")
    wait(j_mon)
    println("time taken (total time, wait time, run time): $(times(j_mon))")
    println("")
    println("results:")
    counts = results(j_mon)[2]
    println(counts)
    println("plot coming up...")

    set_terminal("x11")
    #months = Array(String, 0)
    #monnames = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    #for yr in 2006:2010
    #    for m in monnames
    #        push!(months, string(yr, " ", m))
    #    end
    #end
    #months = months[6:46]
    months = 6:46
    colors = ["blue", "red", "green", "violet", "black", "yellow", "magenta", "brown", "cyan", "pink"]
    params = {}
    idx = 0
    for (key, val) in counts
        idx += 1
        push!(params, months); push!(params, val[6:46])
        push!(params, "color"); push!(params, colors[idx])
        push!(params, "legend"); push!(params, key)
        (idx == 10) && (idx = 0)
    end
    push!(params, "title"); push!(params,string(tag, " in ", typ, "s"))
    push!(params, "xlabel"); push!(params, "months")
    push!(params, "ylabel"); push!(params, "counts")
    plot(params...)
end
 
