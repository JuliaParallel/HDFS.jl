# jobs should define:
# 1. find_rec(jc::HdfsJobCtx)
#       returns reader_status, rec, end_pos
# 2. process_rec(rec)
#       process record and accumulate results
# 3. gather_results()
#       collects results accumulated till now
#       called after processing of a block (distributed processing)
#       called after procedding of complete file (local processing)
# 4. init_job_ctx()
# 5. get_job_ctx()
# 6. destroy_job_ctx()


# TODO:
# - distribute file to be loaded to all machines
# - multiple workers per node
# - better block distribution
# - support reduction step?
# - resilience and restartability against node failure

# job context stores HDFS context required for the job


type HdfsJobCtx{T}
    rdr::HdfsReader
    fname::String
    fblk_hosts::Vector{Vector{String}}
    valid::Bool
    next_rec_pos::Int64
    rec::T

    function HdfsJobCtx(hdfs_host::String, hdfs_port::Integer, fname::String)
        fs = hdfs_connect(hdfs_host, hdfs_port)
        file = hdfs_open_file_read(fs, fname)
        finfo = hdfs_get_path_info(fs, fname)
        rdr = HdfsReader(fs, file, finfo, 0)
        jc = new(rdr, fname, hdfs_get_hosts(fs, fname, 0, finfo.size), true, 1)
        finalizer(jc, finalize_hdfs_job_ctx)
        jc
    end
end

function finalize_hdfs_job_ctx(jc::HdfsJobCtx)
    if(jc.valid)
        hdfs_close_file(jc.rdr.fs, jc.rdr.fi)
        finalize_hdfs_fs(jc.rdr.fs)
        jc.valid = false
    end
end



type HdfsJobQueue
    machine::ASCIIString
    hostname::ASCIIString
    ip::ASCIIString
    proc_id::Int
    block_ids::Vector{Int}
    results::Vector{Any}
end

function setup_remotes(machines::Vector{ASCIIString}, preload::String="")
    @assert :ok == addprocs(machines)
    @assert nprocs() == length(machines) + 1

    wd = pwd()
    ips = Array(ASCIIString, length(machines))
    hns = Array(ASCIIString, length(machines))
    for midx in 1:length(machines)
        remotecall_wait(midx+1, cd, wd)
        @assert wd == remotecall_fetch(midx+1, pwd)
        remotecall_wait(midx+1, require, preload)
        ips[midx] = remotecall_fetch(midx+1, getipaddr)
        hns[midx] = remotecall_fetch(midx+1, gethostname)
    end
    (ips, hns)
end

function find_wrkr_id(host_or_ip::ASCIIString, machines::Vector{ASCIIString}, hns::Vector{ASCIIString}, ips::Vector{ASCIIString})
    for idx in 1:length(machines)
        ((machines[idx] == host_or_ip) || (hns[idx] == host_or_ip) || (ips[idx] == host_or_ip)) && (return (idx+1))
    end
    1  # return namenode location if we do not have workers at required node
end

function setup_queue(jc::HdfsJobCtx, machines::Vector{ASCIIString}, ips::Vector{ASCIIString}, hns::Vector{ASCIIString})
    nnodes = length(machines)+1 # +1 for the namenode, which holds unclaimed blocks to be picked up by any node that finishes early
    blkids = [ Array(Int, 0) for i in 1:nnodes ]

    println("total $(length(jc.fblk_hosts)) blocks to process...")
    for blk_id in 1:length(jc.fblk_hosts)
        push!(blkids[find_wrkr_id(jc.fblk_hosts[blk_id][1], machines, hns, ips)], blk_id)
    end

    jqarr = Array(HdfsJobQueue, nnodes)
    jqarr[1] = HdfsJobQueue("", "", "", 1, blkids[1], Array(Any, 0))
    for idx in 2:nnodes
        jqarr[idx] = HdfsJobQueue(machines[idx-1], hns[idx-1], ips[idx-1], idx, blkids[idx], Array(Any, 0))
    end
    jqarr
end


# schedule blocks to the machines
# if a machine finished blocks local to it, it is allocated blocks on machines which don't have local processor
# TODO: need better allocation strategy
function process_queue(jqarr::Vector{HdfsJobQueue})
    num_done = 0
    @sync begin
        for jq in jqarr
            @async begin
                while((length(jq.block_ids) + length(jqarr[1].block_ids)) > 0)
                    blk_to_proc = shift!(((length(jq.block_ids) > 0) ? jq : jqarr[1]).block_ids)
                    remotecall_wait(jq.proc_id, hdfs_do_job_block_id, blk_to_proc)
                    ret = remotecall_fetch(jq.proc_id, Main.gather_results)
                    push!(jq.results, (blk_to_proc, ret))
                    num_done += 1
                    println("block completed at node $(jq.proc_id). total $(num_done)")
                end
                println("worker node $(jq.proc_id) completed")
            end
        end
    end
end

function hdfs_do_job_block_id(blk_to_proc::Integer)
    jc = Main.get_job_ctx()
    reset_pos(jc.rdr, blk_to_proc)
    hdfs_process_block_buff(jc)
end


function hdfs_process_block_buff(jc::HdfsJobCtx)
    final_pos = length(jc.rdr.cv)

    while(jc.next_rec_pos <= final_pos)
        #println("jc.next_rec_pos = $(jc.next_rec_pos), final_pos = $(final_pos)")
        if(:ok == Main.find_rec(jc))
            #println("calling process_rec")
            Main.process_rec(jc.rec)
        else
            println("could not detect a record!!")
        end
    end
end

function hdfs_job_do_parallel(machines::Vector{ASCIIString}, command_file::String)
    Main.init_job_ctx()
    jc = Main.get_job_ctx()

    ips, hns = setup_remotes(machines, command_file)
    jqarr::Vector{HdfsJobQueue} = setup_queue(jc, machines, ips, hns)

    for jq in jqarr
        (jq.proc_id != myid()) && remotecall_wait(jq.proc_id, Main.init_job_ctx)
    end

    process_queue(jqarr)

    for jq in jqarr
        (jq.proc_id != myid()) && remotecall_wait(jq.proc_id, Main.destroy_job_ctx)
    end
    Main.destroy_job_ctx()
    [ jq.results for jq in jqarr ]
end

# process a complete file (all blocks) at a single node
function hdfs_job_do_serial()
    Main.init_job_ctx()
    jc = Main.get_job_ctx()

    for blk_id in 1:length(jc.fblk_hosts)
        println("processing block $blk_id");
        #println("$(int(start_pos*100/sz)): reading block of len $len at $start_pos of $sz")
        reset_pos(jc.rdr, blk_id)
        #println("reset pos")
        hdfs_process_block_buff(jc)
    end
    Main.destroy_job_ctx()
    Main.gather_results()
end

