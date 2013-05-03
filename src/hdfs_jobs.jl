# jobs should define:
# 1. fn_find_rec(jc::HdfsJobCtx)
#       returns reader_status, rec, end_pos
# 2. fn_map(jc::JobContext)
#       process record and accumulate results
# 3. fn_reduce(jc::JobContext, [map results...])
#       combine results of multiple maps
# 4. fn_summarize(output of fn_reduce)

# TODO:
# - JobContext should store job results and a job status flag
# - Firing a job should be async, with fetch giving the jobcontext/result back
# - multiple workers per node
# - better block distribution
# - support reduction step?
# - resilience and restartability against node failure

# job context stores HDFS context required for the job

const hdfs_jobstore = Dict{Int, Any}()
_hdfs_next_job_id = 1

type HdfsJobCtx
    rdr::HdfsReader
    fname::String
    fblk_hosts::Vector{Vector{String}}
    valid::Bool
    next_rec_pos::Int64
    rec::Union(Any,Nothing)
    results::Union(Any,Nothing)
    fn_find_rec::Function
    fn_map::Function
    fn_reduce::Function
    fn_summarize::Function

    function HdfsJobCtx(hdfs_host::String, hdfs_port::Integer, fname::String, fn_find_rec::Function, fn_map::Function, fn_reduce::Function, fn_summarize::Function)
        fs = hdfs_connect(hdfs_host, hdfs_port)
        file = hdfs_open_file_read(fs, fname)
        finfo = hdfs_get_path_info(fs, fname)
        rdr = HdfsReader(fs, file, finfo, 0)
        jc = new(rdr, fname, hdfs_get_hosts(fs, fname, 0, finfo.size), true, 1, nothing, nothing, fn_find_rec, fn_map, fn_reduce, fn_summarize)
        finalizer(jc, finalize_hdfs_job_ctx)
        jc
    end
end

function finalize_hdfs_job_ctx(jc::HdfsJobCtx)
    if(jc.valid)
        hdfs_close_file(jc.rdr.fs, jc.rdr.fi)
        jc.valid = false
    end
end

function hdfs_next_job_id()
    global _hdfs_next_job_id
    job_id = _hdfs_next_job_id
    _hdfs_next_job_id += 1
    job_id
end

function hdfs_init_job(job_id::Int, hdfs_host::String, hdfs_port::Integer, fname::String, fn_find_rec::Function, fn_map::Function, fn_reduce::Function, fn_summarize::Function)
    global hdfs_jobstore
    jc = HdfsJobCtx(hdfs_host, hdfs_port, fname, fn_find_rec, fn_map, fn_reduce, fn_summarize)
    hdfs_jobstore[job_id] = jc
    :ok
end

function hdfs_destroy_job(job_id::Int)
    global hdfs_jobstore
    if(haskey(hdfs_jobstore, job_id))
        jc = hdfs_jobstore[job_id]
        delete!(hdfs_jobstore, job_id)
        finalize_hdfs_job_ctx(jc)
    end
    :ok
end

function hdfs_job(job_id::Int)
    global hdfs_jobstore
    jc = hdfs_jobstore[job_id]
end

function hdfs_job_results(job_id::Int)
    jc = hdfs_job(job_id)
    jc.results
end

type HdfsJobQueue
    machine::ASCIIString
    hostname::ASCIIString
    ip::ASCIIString
    proc_id::Int
    block_ids::Vector{Int}
    results::Any
    function HdfsJobQueue(machine, hostname, ip, proc_id, block_ids)
        new(machine, hostname, ip, proc_id, block_ids)
    end
end

function prep_remotes()
    n = nprocs()
    machines = Array(ASCIIString, n-1)
    ips = Array(ASCIIString, n-1)
    hns = Array(ASCIIString, n-1)
    wd = pwd()
    for midx in 2:n
        remotecall_wait(midx, cd, wd)
        @assert wd == remotecall_fetch(midx, pwd)
        machines[midx-1] = Base.worker_from_id(midx).host
        ips[midx-1] = remotecall_fetch(midx, getipaddr)
        hns[midx-1] = remotecall_fetch(midx, gethostname)
    end
    (machines, ips, hns)
end


function find_wrkr_id(host_or_ip::ASCIIString, machines::Vector{ASCIIString}, hns::Vector{ASCIIString}, ips::Vector{ASCIIString})
    for idx in 1:length(machines)
        ((machines[idx] == host_or_ip) || (hns[idx] == host_or_ip) || (ips[idx] == host_or_ip)) && (return (idx+1))
    end
    1  # return namenode location if we do not have workers at required node
end


# TODO:
# queue should index blocks with 
function setup_queue(jc::HdfsJobCtx, machines::Vector{ASCIIString}, ips::Vector{ASCIIString}, hns::Vector{ASCIIString})
    nnodes = length(machines)+1 # +1 for the namenode, which holds unclaimed blocks to be picked up by any node that finishes early
    blkids = [ Array(Int, 0) for i in 1:nnodes ]

    println("total $(length(jc.fblk_hosts)) blocks to process...")
    for blk_id in 1:length(jc.fblk_hosts)
        push!(blkids[find_wrkr_id(jc.fblk_hosts[blk_id][1], machines, hns, ips)], blk_id)
    end

    jqarr = Array(HdfsJobQueue, nnodes)
    jqarr[1] = HdfsJobQueue("", "", "", 1, blkids[1])
    for idx in 2:nnodes
        jqarr[idx] = HdfsJobQueue(machines[idx-1], hns[idx-1], ips[idx-1], idx, blkids[idx])
    end
    jqarr
end


# schedule blocks to the machines
# if a machine finished blocks local to it, it is allocated blocks on machines which don't have local processor
# TODO: need better allocation strategy
function process_queue(job_id::Int, jqarr::Vector{HdfsJobQueue})
    jql = length(jqarr)
    num_dones = zeros(Int, jql)
    tot_dones = zeros(Int, 1)
    @sync begin
        for jqidx in 1:jql
            jq = jqarr[jqidx]
            @async begin
                while((length(jq.block_ids) + length(jqarr[1].block_ids)) > 0)
                    blk_to_proc = shift!(((length(jq.block_ids) > 0) ? jq : jqarr[1]).block_ids)
                    remotecall_wait(jq.proc_id, HDFS.hdfs_do_job_block_id, job_id, blk_to_proc)
                    num_dones[jqidx] += 1
                    tot_dones[1] += 1
                    println("job $(job_id): $(num_dones[jqidx]) blocks completed at node $(jq.proc_id). total $(tot_dones[1])")
                end
                jq.results = remotecall_fetch(jq.proc_id, hdfs_job_results, job_id)
                println("job $(job_id): worker node $(jq.proc_id) completed")
            end
        end
    end
end

# TODO: should take filename and block id.
# if jc does not have the named file open, open it and read relevant block
# the scheduler would try and schedule a processor for a file that is already open 
function hdfs_do_job_block_id(job_id::Int, blk_to_proc::Integer)
    jc = hdfs_job(job_id)
    reset_pos(jc.rdr, blk_to_proc)
    jc.next_rec_pos = int64(1)
    hdfs_process_block_buff(jc)
end


function hdfs_process_block_buff(jc::HdfsJobCtx)
    final_pos = length(jc.rdr.cv)

    recs = 0
    while(jc.next_rec_pos <= final_pos)
        #println("jc.next_rec_pos = $(jc.next_rec_pos), final_pos = $(final_pos)")
        if(:ok == jc.fn_find_rec(jc))
            #println("calling process_rec")
            jc.fn_map(jc)
            recs += 1
        else
            println((recs == 0) ? "no usable record in block" : "no usable record at end of block")
        end
    end
end

function hdfs_do_job(hdfs_host::String, hdfs_port::Integer, fname::String, fn_find_rec::Function, fn_map::Function, fn_reduce::Function=(x,y)->y, fn_summarize::Function=(x)->nothing)
    job_id = hdfs_next_job_id()
    hdfs_init_job(job_id, hdfs_host, hdfs_port, fname, fn_find_rec, fn_map, fn_reduce, fn_summarize)
    jc = hdfs_job(job_id)

    #ips, hns = setup_remotes(machines, command_file)
    machines, ips, hns = prep_remotes()
    println(hcat(machines, ips, hns))

    jqarr = setup_queue(jc, machines, ips, hns)

    for jq in jqarr
        (jq.proc_id != myid()) && remotecall_wait(jq.proc_id, hdfs_init_job, job_id, hdfs_host, hdfs_port, fname, fn_find_rec, fn_map, fn_reduce, fn_summarize)
    end

    process_queue(job_id, jqarr)

    for jq in jqarr
        (jq.proc_id != myid()) && remotecall_wait(jq.proc_id, hdfs_destroy_job, job_id)
    end
    res = jc.fn_reduce(jc, filter((x)->(x!=nothing), [x.results for x in jqarr]))
    jc.fn_summarize(res)
    hdfs_destroy_job(job_id)
    res
end

