##
# sample that plots usage trend of any hashtag, url, or smiley string over time.
#
# 1. bring up julia on all hadoop data nodes
#       julia --machinefile ${HADOOP_HOME}/conf/slaves
# 2. load this file (assuming file present in cwd)
#       julia> require("twitter_test_smileys.jl")
# 3. we would use the daily twitter summary data from infochimps 
#    data source: http://www.infochimps.com/datasets/twitter-census-conversation-metrics-one-year-of-urls-hashtags-sm--2
#    ensure that the daily summary file is available at /twitter_daily_summary.tsv
#
# 4. run the test function
#    julia> do_plot_counts("hdfs://host:port/twitter_daily_summary.tsv")
# 

using HDFS
using Gaston

##
# find smiley records from HDFS CSV file
find_count_of_typ(r::HdfsReader, next_rec_pos, ttyp::String) = HDFS.hdfs_find_rec_csv(r, next_rec_pos, '\n', '\t', 1024, (ttyp, nothing, nothing, nothing))

function map_count_monthly(rec, tag::Regex, combine::Bool)
    ((nothing == rec) || (length(rec) == 0) || !ismatch(tag, rec[4])) && return []
    println(rec)

    ts = rec[2]
    ts_year = int(ts[1:4])
    ts_mon = int(ts[5:6])
    month_idx = 12*(ts_year-2006) + ts_mon  # twitter begun from 2006
    [(combine ? tag.pattern : rec[4], month_idx, int(rec[3]))]
end

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

reduce_count_monthly(reduced, results...) = HDFS.reduce_dicts(+, reduced, results...)


function do_plot_counts(furl::String, typ::String, tag::String)
    println("starting dmapreduce...")
    j_mon = dmapreduce(MRFileInput([furl], (x,y)->find_count_of_typ(x,y,typ)), x->map_count_monthly(x, Regex(tag), true), collect_count_monthly, reduce_count_monthly)

    println("waiting for dmapreduce to finish...")
    loopstatus = true
    while(loopstatus)
        sleep(2)
        jstatus,jstatusinfo = status(j_mon,true)
        ((jstatus == "error") || (jstatus == "complete")) && (loopstatus = false)
        (jstatus == "running") && println("$(j_mon): $(jstatusinfo)% complete...")
    end
    wait(j_mon)
    println("time taken (total time, wait time, run time): $(times(j_mon))")
    println("")
    println("results:")
    counts = results(j_mon)[2]
    println(counts)
    ((nothing == counts) || (0 == length(counts))) && return

    monnames = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
    println("maximums:")
    for (key,val) in counts
        maxmon = indmax(val)
        maxyear = 2006 + int(floor(maxmon/12))
        maxmonname = ((maxmon-1)%12) + 1
        println("$(key): $(monnames[maxmonname]) $(maxyear)")
    end

    println("plot coming up...")

    set_terminal("x11")
    #months = Array(String, 0)
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
 
