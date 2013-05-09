
typealias JobId     Int64
typealias FuncNone  Union(Function,Nothing)
typealias MRSource  Union(String,JobId)


##
# Assign Job Ids. A one up number
global __next_job_id = int64(0)::JobId
function _next_job_id()
    global __next_job_id
    __next_job_id += 1
end

##
# Keep an account of the worker machines available
const _remotes = Array(Vector{ASCIIString},0)
const _all_remote_names = Set{String}()
_num_remotes() = length(_remotes[1])
function __prep_remotes()
    global _remotes
    global _all_remote_names

    empty!(_remotes)
    empty!(_all_remote_names)

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

    for x in (machines, ips, hns)
        push!(_remotes, x)
        union!(_all_remote_names, x)
        unshift!(x, "")     # to represent the default worker node
    end
end

##
# Methods used only at worker nodes
function _worker_init_job(jid::JobId, rdr_typ::DataType, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
    global _job_store
    global _def_wrkr_job_store
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid] = HdfsJobCtx(rdr_typ, fn_find_rec, fn_map, fn_collect, fn_reduce)
end

function _worker_unload(jid::JobId)
    global _job_store
    global _def_wrkr_job_store
    d = ((myid() == 1) ? _def_wrkr_job_store : _job_store)
    delete!(d, jid)
    jid
end

function _worker_fetch_collected(jid::JobId) 
    global _job_store
    global _def_wrkr_job_store

    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid].info.results
end

function _worker_map_chunk(jid::JobId, fname::String, blk_id)
    global _job_store
    global _def_wrkr_job_store

    j = ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid]
    jinfo = j.info
    reset_pos(jinfo.rdr, fname, blk_id)
    jinfo.next_rec_pos = 1
    
    chunk_len = block_sz(jinfo.rdr, blk_id)
    recs = 0
    fn_find_rec = j.fn_find_rec
    fn_map = j.fn_map
    fn_collect = j.fn_collect
    while(jinfo.next_rec_pos <= chunk_len)
        if(:ok == fn_find_rec(j))
            fn_map(j)
            fn_collect(j)
            recs += 1
        else
            println((recs == 0) ? "no usable record in block" : "no usable record at end of block")
        end
    end
end


##
# scheduler function
const _feeders = Dict{Int,RemoteRef}()
function _sched()
    global _remotes
    # TODO: recalculate priorities and reorder _machine_jobparts

    # start feeder tasks if required
    for procid in 1:_num_remotes()
        haskey(_feeders, procid) && !isready(_feeders[procid]) && continue
        machine, ip, hn = map(x->x[procid], _remotes)
        (nothing == __fetch_tasks(machine, ip, hn, true)) && continue
        _feeders[procid] = @async _push_to_worker(procid)
    end
end

function __fetch_tasks(machine::String, ip::String, hn::String, peek::Bool=false)
    global _machine_jobparts
    global _jobpart_machines

    v = nothing
    haskey(_machine_jobparts, machine) && (v = _machine_jobparts[machine])
    ((v == nothing) || (0 == length(v))) && haskey(_machine_jobparts, ip) && (v = _machine_jobparts[ip])
    ((v == nothing)  || (0 == length(v))) && haskey(_machine_jobparts, hn) && (v = _machine_jobparts[hn])
    ((v == nothing)  || (0 == length(v))) && return nothing

    peek && (return ((length(v) > 0) ? length(v) : nothing))

    # get the first one in _machine_jobparts
    jobpart = shift!(v)

    # unschedule from other machines
    other_macs = delete!(_jobpart_machines, jobpart)
    for mac in other_macs
        filter!(x->(x != jobpart), _machine_jobparts[mac])
    end

    jobpart
end

function _push_to_worker(procid)
    global _remotes
    global _job_store

    machine, ip, hn = map(x->x[procid], _remotes)

    while(true)
        # if no tasks, exit task
        jobpart = __fetch_tasks(machine, ip, hn)
        (jobpart == nothing) && return
       
        # TODO: jobpart may be a command as well 
        jid, fname, blk_id = jobpart

        # call method at worker
        x = remotecall_fetch(procid, HDFS._worker_map_chunk, jid, fname, blk_id)

        # update the job id
        j = _job_store[jid] 
        if(isa(x, Exception))
            j.info.state = STATE_ERROR
        else
            j.info.num_parts_done += 1
        end
        if(j.info.num_parts == j.info.num_parts_done)
            try
                if(j.fn_reduce != nothing)
                    # do the reduce step
                    # TODO: distributed reduction and not plugged in at this place
                    for procid in 1:_num_remotes()
                        red = remotecall_fetch(procid, HDFS._worker_fetch_collected, jid)
                        j.info.red = j.fn_reduce(j.info.red, red)
                    end
                end
                j.info.state = STATE_COMPLETE
            catch
                j.info.state = STATE_ERROR
            end
        end
    end
end


##
# Job store and scheduler
type HdfsJobSchedInfo
    source_spec::MRSource                                   # url or url with wildcard or job_id
    source::Union(Int,Vector)                               # job_id, file or list of files
    fblk_hosts::Dict{String, Vector{Vector{String}}}        # block distribution for source files
    num_parts::Int
    num_parts_done::Int
    state::Int
    red::Union(Any,Nothing)
end

type HdfsJobRunInfo
    rdr::Union(HdfsReader,Nothing)
    next_rec_pos::Int64
    rec::Union(Any,Nothing)
    results::Union(Any,Nothing)
end

type HdfsJobCtx
    fn_find_rec::Function
    fn_map::Function
    fn_collect::Function
    fn_reduce::FuncNone

    info::Union(HdfsJobRunInfo, HdfsJobSchedInfo)           # holds sched info at master node and run info at worker nodes

    # constructor at the scheduler end
    function HdfsJobCtx(source::MRSource, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        # break up source to host, port, file path
        comps = urlparse(source)
        uname = username(comps)
        hname = hostname(comps)
        portnum = port(comps)
        fname = comps.url

        # TODO: handle jobid
        # TODO: handle wild cards
        fs = (nothing == uname) ? hdfs_connect(hname, portnum) : hdfs_connect_as_user(hname, portnum, uname)
        d = Dict{String, Vector{Vector{String}}}()
        finfo = hdfs_get_path_info(fs, fname)
        blkdist = hdfs_get_hosts(fs, fname, 0, finfo.size) 
        d[source] = blkdist
        new(fn_find_rec, fn_map, fn_collect, fn_reduce, HdfsJobSchedInfo(source, [source], d, length(blkdist), 0, STATE_STARTING, nothing))
    end

    # constructor at the worker end
    function HdfsJobCtx(rdr_typ::DataType, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        rdr = (rdr_typ == HdfsReader) ? HdfsReader() : nothing
        new(fn_find_rec, fn_map, fn_collect, fn_reduce, HdfsJobRunInfo(rdr,0,nothing,nothing))
    end
end

const _def_wrkr_job_store = Dict{JobId, HdfsJobCtx}()
const _job_store = Dict{JobId, HdfsJobCtx}()
const _jobpart_machines = Dict{Tuple,Vector{ASCIIString}}()
const _machine_jobparts = Dict{ASCIIString, Vector{Tuple}}()

const STATE_ERROR = 0
const STATE_STARTING = 1
const STATE_RUNNING = 2
const STATE_COMPLETE = 3

# remove machines where workers are not running
# remap to other machines (currently remapping to "")
function __remap_macs_to_procs(macs)
    global _all_remote_names

    available_macs = filter(x->contains(_all_remote_names, x), macs)
    (length(available_macs) == 0) && push!(available_macs, "")
    available_macs
end

function _distribute(jid::JobId, j::HdfsJobCtx)
    global _jobpart_machines
    global _machine_jobparts

    sch::HdfsJobSchedInfo = j.info
    for fname in sch.source
        blk_dist = sch.fblk_hosts[fname]
        for blk_id in 1:length(blk_dist)
            macs = blk_dist[blk_id]
            macs = __remap_macs_to_procs(macs)
            t = (jid, fname, blk_id)
            _jobpart_machines[t] = macs
            for mac in macs
                # TODO: notmalize hostnames to ips
                try
                    push!(_machine_jobparts[mac], t)
                catch
                    _machine_jobparts[mac] = [t]
                end
            end
        end
    end
end

function mapreduce(source::MRSource, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone=nothing)
    global _remotes
    global _job_store

    (0 == length(_remotes)) && __prep_remotes()

    # create and set a job ctx
    j = HdfsJobCtx(source, fn_find_rec, fn_map, fn_collect, fn_reduce)
    jid = _next_job_id()
    _job_store[jid] = j

    @async begin
        try 
            # init job at all workers
            @sync begin
                for procid in 1:_num_remotes()
                    @async begin
                        try 
                            println("initializing job $jid at $procid")
                            # TODO: look at source and assign appropriate open method
                            remotecall_wait(procid, HDFS._worker_init_job, jid, HdfsReader, fn_find_rec, fn_map, fn_collect, fn_reduce)
                            println("initialized job $jid at $procid")
                        catch
                            println("error initializing job $jid at $procid")
                            j.info.state = STATE_ERROR
                        end
                    end
                end
            end

            # once created at all workers...
            println("distributing blocks for job $jid")
            _distribute(jid, j)
            println("distributed blocks for job $jid")
            j.info.state = STATE_RUNNING
            _sched()
            println("scheduled blocks for job $jid")
        catch
            println("error starting job $jid")
            j.info.state = STATE_ERROR
        end
    end
    jid
end

function status(jid::JobId)
    global _job_store
    status(_job_store[jid])
end

function status(j::HdfsJobCtx)
    (j.info.state == STATE_RUNNING) && (return "running")
    (j.info.state == STATE_COMPLETE) && (return "complete")
    (j.info.state == STATE_STARTING) && (return "starting")
    (j.info.state == STATE_ERROR) && (return "error")
    error("job in unknown state")
end

function results(jid::JobId) 
    global _job_store
    j = _job_store[jid]
    (status(j), j.info.red)
end

function unload(jid::JobId)
    global _job_store
    j = _job_store[jid]
    (j.info.state == STATE_RUNNING) && error("can't unload running job")

    for procid in 1:_num_remotes()
        remotecall(procid, HDFS._worker_unload, jid)
    end
    delete!(_job_store, jid)
    jid
end 


