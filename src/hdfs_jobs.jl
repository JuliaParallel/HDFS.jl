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

const hdfs_jobstore = Dict{Int, Any}()
_hdfs_next_job_id = 1

type HdfsJobCtx{Tr, Tc}
    rdr::HdfsReader
    fname::String
    fblk_hosts::Vector{Vector{String}}
    valid::Bool
    next_rec_pos::Int64
    rec::Tr
    results::Tc

    function HdfsJobCtx(hdfs_host::String, hdfs_port::Integer, fname::String, rec::Tr, results::Tc)
        fs = hdfs_connect(hdfs_host, hdfs_port)
        file = hdfs_open_file_read(fs, fname)
        finfo = hdfs_get_path_info(fs, fname)
        rdr = HdfsReader(fs, file, finfo, 0)
        jc = new(rdr, fname, hdfs_get_hosts(fs, fname, 0, finfo.size), true, 1, rec, results)
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

function hdfs_next_job_id()
    global _hdfs_next_job_id
    job_id = _hdfs_next_job_id
    _hdfs_next_job_id += 1
    job_id
end

function hdfs_init_job(job_id::Int, hdfs_host::String, hdfs_port::Integer, fname::String, rec::Any, results::Any)
    global hdfs_jobstore
    tc = typeof(results)
    tr = typeof(rec)
    jc = HdfsJobCtx{tr,tc}(hdfs_host, hdfs_port, fname, rec, results)
    hdfs_jobstore[job_id] = jc
    :ok
end

function hdfs_destroy_job(job_id::Int)
    global hdfs_jobstore
    if(has(hdfs_jobstore, job_id))
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

type HdfsJobQueue{Tc}
    machine::ASCIIString
    hostname::ASCIIString
    ip::ASCIIString
    proc_id::Int
    block_ids::Vector{Int}
    results::Tc
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


function setup_queue{Tr,Tc}(jc::HdfsJobCtx{Tr,Tc}, machines::Vector{ASCIIString}, ips::Vector{ASCIIString}, hns::Vector{ASCIIString})
    nnodes = length(machines)+1 # +1 for the namenode, which holds unclaimed blocks to be picked up by any node that finishes early
    blkids = [ Array(Int, 0) for i in 1:nnodes ]

    println("total $(length(jc.fblk_hosts)) blocks to process...")
    for blk_id in 1:length(jc.fblk_hosts)
        push!(blkids[find_wrkr_id(jc.fblk_hosts[blk_id][1], machines, hns, ips)], blk_id)
    end

    jqarr = Array(HdfsJobQueue, nnodes)
    jqarr[1] = HdfsJobQueue{Tc}("", "", "", 1, blkids[1])
    for idx in 2:nnodes
        jqarr[idx] = HdfsJobQueue{Tc}(machines[idx-1], hns[idx-1], ips[idx-1], idx, blkids[idx])
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
                    println("$(num_dones[jqidx]) blocks completed at node $(jq.proc_id). total $(tot_dones[1]) of $(jql)")
                end
                jq.results = remotecall_fetch(jq.proc_id, hdfs_job_results, job_id)
                println("worker node $(jq.proc_id) completed")
            end
        end
    end
end

function hdfs_do_job_block_id(job_id::Int, blk_to_proc::Integer)
    jc = hdfs_job(job_id)
    reset_pos(jc.rdr, blk_to_proc)
    hdfs_process_block_buff(jc)
end


function hdfs_process_block_buff(jc::HdfsJobCtx)
    final_pos = length(jc.rdr.cv)

    while(jc.next_rec_pos <= final_pos)
        #println("jc.next_rec_pos = $(jc.next_rec_pos), final_pos = $(final_pos)")
        if(:ok == Main.find_rec(jc))
            #println("calling process_rec")
            Main.process_rec(jc)
        else
            println("could not detect a record!!")
        end
    end
end

function hdfs_do_job{Tr,Tc}(hdfs_host::String, hdfs_port::Integer, fname::String, rec::Tr, results::Tc)
    job_id = hdfs_next_job_id()
    hdfs_init_job(job_id, hdfs_host, hdfs_port, fname, rec, results)
    jc = hdfs_job(job_id)

    #ips, hns = setup_remotes(machines, command_file)
    machines, ips, hns = prep_remotes()
    println(hcat(machines, ips, hns))

    jqarr = setup_queue(jc, machines, ips, hns)

    for jq in jqarr
        (jq.proc_id != myid()) && remotecall_wait(jq.proc_id, hdfs_init_job, job_id, hdfs_host, hdfs_port, fname, rec, results)
    end

    process_queue(job_id, jqarr)

    for jq in jqarr
        (jq.proc_id != myid()) && remotecall_wait(jq.proc_id, hdfs_destroy_job, job_id)
    end
    res = Main.reduce(jc, convert(Vector{Tc}, [x.results for x in jqarr]))
    Main.summarize(res)
    hdfs_destroy_job(job_id)
    res
end

# process a complete file (all blocks) at a single node
# hdfs_do_job with a single node processes all blocks locally
# this is not required
#function hdfs_job_do_serial(hdfs_host::String, hdfs_port::Integer, fname::String, rec::Any, results::Any)
#    job_id = hdfs_next_job_id()
#    hdfs_init_job(job_id, hdfs_host, hdfs_port, fname, rec, results)
#    jc = hdfs_job(job_id)
#
#    for blk_id in 1:length(jc.fblk_hosts)
#        println("processing block $blk_id");
#        #println("$(int(start_pos*100/sz)): reading block of len $len at $start_pos of $sz")
#        reset_pos(jc.rdr, blk_id)
#        #println("reset pos")
#        hdfs_process_block_buff(jc)
#    end
#    res = Main.reduce(jc, [jc.results])
#    Main.summarize(res)
#    hdfs_destroy_job(job_id)
#    res
#end

