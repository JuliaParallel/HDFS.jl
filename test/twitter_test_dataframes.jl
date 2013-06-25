##
# sample test program to produce monthly aggregate count of smileys
# used in tweets between year 2006 to 2009, based on data from infochimps.

using HDFS
using Gaston

##
# find smiley records from HDFS CSV file
find_smiley_df(r, next_rec_pos) = HDFS.hdfs_find_rec_table(r, next_rec_pos, '\n', '\t')

##
# reduce smiley counts or array of counts
reduce_smiley_df(reduced, results...) = +((nothing == reduced) ? 0 : reduced, results...)


##
# for finding total counts across all years
function map_total(rec)
    #println("map got $rec")
    (nothing == rec) && return []
    [sum(rec[3])]
end

function collect_total(results, rec)
    #println("collect got $rec")
    (length(rec) == 0) && return results
    (results == nothing) ? rec : (results+rec)
end

function do_dataframe_test(furl::String)
    println("starting dmapreduce job...")
    j = dmapreduce(MRFileInput([furl], find_smiley_df, "stream"), map_total, collect_total, reduce_smiley_df)
    println("waiting for jobs to finish...")
    loopstatus = true
    while(loopstatus)
        sleep(2)
        jstatus,jstatusinfo = status(j,true)
        ((jstatus == "error") || (jstatus == "complete")) && (loopstatus = false)
        (jstatus == "running") && println("$(j): $(jstatusinfo)% complete...")
    end

    wait(j)
    println("time taken (total time, wait time, run time): $(times(j))")
    println("")
    jstatus,total_count = results(j)
    println("completed with status: $jstatus")
    if(jstatus == "complete")
        println("results:")
        println("total number of smileys: $total_count")
    else
        println(status(j, true))
    end
end

