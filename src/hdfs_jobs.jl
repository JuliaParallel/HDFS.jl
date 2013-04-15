# jobs should define:
# 1. find_record(buff::Array{Uint8,1}, start_pos::Int64, len::Int64)
#       returns rec, end_pos
# 2. process_record(rec::Any)
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
type HdfsJobCtx
    fs::HdfsFS
    fname::String
    fi::HdfsFile
    finfo::HdfsFileInfo
    fblk_hosts::Array{Array{String,1},1}
    buff::Array{Uint8,1}
    valid::Bool

    function HdfsJobCtx(hdfs_host::String, hdfs_port::Integer, fname::String)
        local fs::HdfsFS = hdfs_connect(hdfs_host, hdfs_port)
        local file::HdfsFile = hdfs_open_file_read(fs, fname)
        local finfo::HdfsFileInfo = hdfs_get_path_info(fs, fname)
        jc = new(fs, fname, file, finfo, hdfs_get_hosts(fs, fname, 0, finfo.size), Array(Uint8, finfo.block_sz), true)
        finalizer(jc, finalize_hdfs_job_ctx)
        jc
    end
end

function finalize_hdfs_job_ctx(jc::HdfsJobCtx)
    if(jc.valid)
        hdfs_close_file(jc.fs, jc.fi)
        finalize_hdfs_fs(jc.fs)
        jc.valid = false
    end
end




type HdfsJobQueue
    machine::ASCIIString
    hostname::ASCIIString
    ip::ASCIIString
    proc_id::Int
    block_ids::Array{Int,1}
    results::Array{Any,1}
end

function setup_remotes(machines::Array{ASCIIString,1}, preload::String="")
    @assert :ok == addprocs(machines)
    @assert nprocs() == length(machines) + 1

    local wd::String = pwd()
    local ips = Array(ASCIIString, length(machines))
    local hns = Array(ASCIIString, length(machines))
    for midx in 1:length(machines)
        remotecall_wait(midx+1, cd, wd)
        @assert wd == remotecall_fetch(midx+1, pwd)
        remotecall_wait(midx+1, require, preload)
        ips[midx] = remotecall_fetch(midx+1, getipaddr)
        hns[midx] = remotecall_fetch(midx+1, gethostname)
    end
    (ips, hns)
end

function find_wrkr_id(host_or_ip::ASCIIString, machines::Array{ASCIIString,1}, hns::Array{ASCIIString,1}, ips::Array{ASCIIString,1})
    for idx in 1:length(machines)
        ((machines[idx] == host_or_ip) || (hns[idx] == host_or_ip) || (ips[idx] == host_or_ip)) && (return (idx+1))
    end
    1  # return namenode location if we do not have workers at required node
end

function setup_queue(jc::HdfsJobCtx, machines::Array{ASCIIString,1}, ips::Array{ASCIIString,1}, hns::Array{ASCIIString,1})
    local nnodes = length(machines)+1 # +1 for the namenode, which holds unclaimed blocks to be picked up by any node that finishes early
    local blkids::Array{Array{Int,1},1} = [ Array(Int, 0) for i in 1:nnodes ]

    println("total $(length(jc.fblk_hosts)) blocks to process...")
    for blk_id in 1:length(jc.fblk_hosts)
        push!(blkids[find_wrkr_id(jc.fblk_hosts[blk_id][1], machines, hns, ips)], blk_id)
    end

    local jqarr::Array{HdfsJobQueue,1} = Array(HdfsJobQueue, nnodes)
    jqarr[1] = HdfsJobQueue("", "", "", 1, blkids[1], Array(Any, 0))
    for idx in 2:nnodes
        jqarr[idx] = HdfsJobQueue(machines[idx-1], hns[idx-1], ips[idx-1], idx, blkids[idx], Array(Any, 0))
    end
    jqarr
end

function process_queue(jqarr::Array{HdfsJobQueue,1})
    local num_done::Int = 0
    @sync begin
        for jq in jqarr
            @async begin
                while((length(jq.block_ids) + length(jqarr[1].block_ids)) > 0)
                    blk_to_proc = 0
                    if(length(jq.block_ids) > 0)
                        blk_to_proc = shift!(jq.block_ids)
                    else 
                        blk_to_proc = shift!(jqarr[1].block_ids)
                    end

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
    local jc::HdfsJobCtx = Main.get_job_ctx()
    local start_pos = (jc.finfo.block_sz)*(blk_to_proc-1)
    local len = min(jc.finfo.block_sz, jc.finfo.size-start_pos)

    hdfs_pread(jc.fs, jc.fi, start_pos, convert(Ptr{Void}, jc.buff), len)
    process_block(jc.buff, len)
end

function hdfs_job_do_parallel(machines::Array{ASCIIString,1}, command_file::String)
    Main.init_job_ctx()
    local jc::HdfsJobCtx = Main.get_job_ctx()

    ips, hns = setup_remotes(machines, command_file)
    jqarr::Array{HdfsJobQueue,1} = setup_queue(jc, machines, ips, hns)

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
    local jc::HdfsJobCtx = Main.get_job_ctx()
    local sz::Int64 = jc.finfo.size
    local start_pos::Int64 = 1
    local end_pos::Int64 = 1
    local len::Int64 = 1

    hdfs_seek(jc.fs, jc.fi, 0)
    while start_pos <= sz
        end_pos = start_pos + jc.finfo.block_sz - 1
        (end_pos > sz) && (end_pos = sz)

        len = end_pos - start_pos + 1
        println("$(int(start_pos*100/sz)): reading block of len $len at $start_pos of $sz")
        hdfs_read(jc.fs, jc.fi, convert(Ptr{Void}, jc.buff), len)
        #println("processing a new block")
        process_block(jc.buff, len)
        start_pos = end_pos+1
    end
    Main.destroy_job_ctx()
    Main.gather_results()
end


function process_block(buff::Array{Uint8,1}, len::Int64)
    #println("processing block of len $len")

    local start_pos::Int64 = 1
    local end_pos::Int64 = 0
    while(start_pos <= len)
        rec, end_pos = Main.find_record(buff, start_pos, len)
        try
            Main.process_record(rec)
        catch
            # TODO: mechanism to fetch complete record from other blocks.
            #       ignore leading bytes. fetch only bytes ahead of current block.
            println("possibly partial record in block. ignoring.")
        end
        start_pos = end_pos+1
    end
end
