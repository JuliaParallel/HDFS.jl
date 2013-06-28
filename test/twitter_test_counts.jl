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
# TRENDS OBSERVED:
#   - flu/swineflu/birdflu: Apr 2009
#   - haloween: Oct
#   - thanksgiving: Oct-Nov
#   - obama: Nov 2008 (elected), Jan 2009 (speech), Oct 2009 (peace prize)
#   - earthquake: Jan 2009 (costarica), Mar 2009 (Tonga), Apr 2009 (italy), Sep 2009 (southern sumatra)
#
# NOTE: 
#   to show plots you need to load one of the following set of modules:
#       - Gaston
#       - Winston and Tk
#
#   If you are using Winston, it may help to modify the following in Winston.ini:
#   ticklabels_offset       = 5
#   ticklabels_style        = {fontsize:1.0, textangle:90}
# 

using HDFS
using HDFS.MapReduce
using DataFrames

##
# find smiley records from HDFS CSV file
find_recs_as_df(r, next_rec_pos) = HDFS.MapReduce.find_rec(r, next_rec_pos, DataFrame, '\n', '\t')


function midx(yyyymmdd)
    yyyymmdd = int(yyyymmdd)
    ypos = (yyyymmdd > 10^9) ? 10^6 : 10^4
    y = int(floor(yyyymmdd/ypos))
    m = int(floor((yyyymmdd - ypos*y)/(ypos/100)))
    12*(y-2006)+m
end
function map_count_monthly(rec, tag_type::String, tag::Regex)
    ((nothing == rec) || (nrow(rec) == 0)) && return []
    #println(rec)

    filtrows = Int[]
    for (i,v) in enumerate(rec[4])
        (tag_type == rec[i,1]) && ismatch(tag,v) && push!(filtrows,i)
    end
    subrec = subset(rec, filtrows)
    (nrow(subrec) == 0) && return []

    subrec = DataFrame({subrec[1], PooledDataArray(map(x->midx(x), subrec[2])), subrec[3], subrec[4]})
    monthsum = by(subrec, :x2, x -> sum(x[:x3]))

    [(tag.pattern, monthsum[idx,1], monthsum[idx,2]) for idx in 1:nrow(monthsum)]
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

reduce_count_monthly(reduced, results...) = HDFS.MapReduce.reduce_dicts(+, reduced, results...)



##
# twitter archive search begin
const colors = ["blue", "red", "green", "violet", "black", "yellow", "magenta", "brown", "cyan", "pink"]
const months = 6:46
const monnames = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"]
const yearmonths = String[]
for y in [2006,2007,2008,2009]
    for m in monnames
        push!(yearmonths, string("$(m) $(y)"))
    end
end

function do_plot_counts(furl::String, typ::String, tag::String)
    function is_loaded(x::Symbol)
        try
            return (Module == typeof(eval(x)))
        catch
            return false
        end
    end

    function plot_winston()
        idx = 0
        win = Toplevel(string(tag, " in ", typ, "s"), 900, 600)
        c = Canvas(win, 900, 600)
        pack(c, expand=true, fill="both")

        p = FramedPlot()
        #setattr(p.x1, "ticklabels", yearmonths[6:5:46])
        #setattr(p.x1, "ticks", 6:5:46)
        setattr(p.x1, "ticklabels", yearmonths[6:46])
        setattr(p.x1, "ticks", 6:46)

        for (key, val) in counts
            idx += 1
            add(p, Curve(months, val[6:46], "color", colors[idx]))
            (idx == 10) && (idx = 0)
        end
        Winston.display(c,p)
    end

    function plot_gaston()
        set_terminal("x11")
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

    function wait_results()
        loopstatus = true
        while(loopstatus)
            sleep(5)
            jstatus,jstatusinfo = status(j_mon,true)
            ((jstatus == "error") || (jstatus == "complete")) && (loopstatus = false)
            (jstatus == "running") && println("$(j_mon): $(jstatusinfo)% complete...")
        end
        wait(j_mon)
        println("time taken (total time, wait time, run time): $(times(j_mon))")
        println("")
    end

    function print_results()
        println(counts)
        ((nothing == counts) || (0 == length(counts))) && return

        println("maximum:")
        for (key,val) in counts
            maxmon = indmax(val)
            maxyear = 2006 + int(floor(maxmon/12))
            maxmonname = ((maxmon-1)%12) + 1
            println("$(key): $(monnames[maxmonname]) $(maxyear)")
        end

        println("local maximas:")
        for (key,val) in counts
            len = length(val)
            avgs = [mean(val[max(1,(n-2)):min(len,(n+2))]) for n in 1:len]
            for n in 1:len
                if((val[n] > 25) && (val[n] > (1.25)*avgs[n]))
                    #println("val $(val[n]), avgs $(avgs[n]), n $(n)")
                    maxyear = 2006 + int(floor((n-1)/12))
                    maxmonname = ((n-1)%12) + 1
                    println("$(key): $(monnames[maxmonname]) $(maxyear)")
                end
            end
        end
        
        counts
    end

    ##
    # function body begin
    println("starting dmapreduce...")
    j_mon = dmapreduce(MRHdfsFileInput([furl], (x,y)->find_recs_as_df(x,y)), x->map_count_monthly(x, "smiley", Regex(tag)), collect_count_monthly, reduce_count_monthly)

    println("waiting for dmapreduce to finish...")
    wait_results()

    println("results:")
    counts = results(j_mon)[2]
    print_results()

    println("plot coming up...")
    is_loaded(:Winston) ? plot_winston() :
    is_loaded(:Gaston) ? plot_gaston() :
    println("can not plot results. no graphics library loaded.")
end

