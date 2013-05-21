
typealias JobId     Int64
typealias FuncNone  Union(Function,Nothing)
typealias MRSource  Union(String,JobId)

global _debug = false

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
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid] = HdfsJobCtx(jid, rdr_typ, fn_find_rec, fn_map, fn_collect, fn_reduce)
    jid
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

function _worker_map_chunk(jid::JobId, chunk_url::String)
    global _job_store
    global _def_wrkr_job_store
    global _debug

    j = ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid]
    jinfo = j.info
    recs = 0
    fn_map = j.fn_map
    fn_collect = j.fn_collect

    results = jinfo.results
    for rec in iterator(jinfo.rdr, chunk_url, j.fn_find_rec)
        (nothing == rec) && continue
        for mapped_rec in fn_map(rec)
            results = fn_collect(results, mapped_rec)
            recs += 1
        end
    end
    jinfo.results = results
    _debug && println((recs == 0) ? "no usable record in block" : "no usable record at end of block")
    recs
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
        (nothing == __fetch_tasks(procid, machine, ip, hn, true)) && continue
        _feeders[procid] = @async _push_to_worker(procid)
    end
end

function __fetch_tasks(proc_id::Int, machine::String, ip::String, hn::String, peek::Bool=false)
    global _machine_jobparts
    global _procid_jobparts
    global _jobpart_machines

    v = nothing
    haskey(_procid_jobparts, proc_id) && (v = _procid_jobparts[proc_id])
    ((v == nothing) || (0 == length(v))) && haskey(_machine_jobparts, machine) && (v = _machine_jobparts[machine])
    ((v == nothing) || (0 == length(v))) && haskey(_machine_jobparts, ip) && (v = _machine_jobparts[ip])
    ((v == nothing) || (0 == length(v))) && haskey(_machine_jobparts, hn) && (v = _machine_jobparts[hn])
    ((v == nothing) || (0 == length(v))) && return nothing

    peek && (return ((length(v) > 0) ? length(v) : nothing))

    # get the first one in _machine_jobparts
    jobpart = shift!(v)

    # unschedule from other machines
    if(haskey(_jobpart_machines, jobpart))
        other_macs = delete!(_jobpart_machines, jobpart)
        for mac in other_macs
            filter!(x->(x != jobpart), _machine_jobparts[mac])
        end
    end

    jobpart
end

function _push_to_worker(procid)
    global _remotes
    global _job_store

    machine, ip, hn = map(x->x[procid], _remotes)

    while(true)
        # if no tasks, exit task
        jobpart = __fetch_tasks(procid, machine, ip, hn)
        (jobpart == nothing) && return
       
        # TODO: jobpart may be a command as well 
        jid, blk_url = jobpart

        # call method at worker
        _debug && println("calling procid $(procid) for jobid $(jid) with url $(blk_url)")
        ret = remotecall_fetch(procid, HDFS._worker_map_chunk, jid, blk_url)
        _debug && println("returned from call to procid $(procid) for jobid $(jid) with url $(blk_url)")

        # update the job id
        j = _job_store[jid] 
        (j.info.state == STATE_ERROR) && continue

        if(!isa(ret, Integer))
            _set_status(j, STATE_ERROR, ret)
        else
            _debug && println("num records mapped: $(ret)")
            j.info.num_parts_done += 1
        end

        if((j.info.state != STATE_ERROR) && (j.info.num_parts == j.info.num_parts_done))
            try
                if(j.fn_reduce != nothing)
                    # do the reduce step
                    # TODO: distributed reduction and not plugged in at this place
                    for procid in 1:_num_remotes()
                        red = remotecall_fetch(procid, HDFS._worker_fetch_collected, jid)
                        isa(red,Exception) && throw(red)
                        j.info.red = j.fn_reduce(j.info.red, red)
                    end
                end
                _set_status(j, STATE_COMPLETE)
            catch ex
                _set_status(j, STATE_ERROR, ex)
            end
        end
    end
end


##
# Job store and scheduler
type HdfsJobSchedInfo
    source_spec::MRSource                                   # job_id or url (optionally with wildcard)
    source::Union(JobId,Vector)                             # job_id or list of files
    dist_info::Any                                          # block distribution for source files
    num_parts::Int
    num_parts_done::Int
    state::Int
    state_info::Any
    red::Any
    trigger::RemoteRef
    begin_time::Float64                                     # submitted
    sched_time::Float64                                     # scheduled (after dependent jobs finish)
    end_time::Float64                                       # completed

    function HdfsJobSchedInfo(source_spec::MRSource, source::Union(JobId,Vector), dist_info::Any, num_parts::Int)
        new(source_spec, source, dist_info, num_parts, 0, STATE_STARTING, nothing, nothing, RemoteRef(), time(), 0.0, 0.0)
    end
end

type HdfsJobRunInfo
    rdr::MapInputReader 
    results::Any
end

type HdfsJobCtx
    jid::JobId
    fn_find_rec::Function
    fn_map::Function
    fn_collect::Function
    fn_reduce::FuncNone

    info::Union(HdfsJobRunInfo, HdfsJobSchedInfo)           # holds sched info at master node and run info at worker nodes

    # constructor at the scheduler end
    function HdfsJobCtx(jid::JobId, source::JobId, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        new(jid, fn_find_rec, fn_map, fn_collect, fn_reduce, HdfsJobSchedInfo(source, source, [], _num_remotes()))
    end

    function HdfsJobCtx(jid::JobId, source::String, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        # break up source to host, port, file path
        comps = urlparse(source)
        uname = username(comps)
        hname = hostname(comps)
        portnum = port(comps)
        fname = comps.url

        # TODO: handle wild cards
        fs = (nothing == uname) ? hdfs_connect(hname, portnum) : hdfs_connect_as_user(hname, portnum, uname)
        d = Dict{String, Vector{Vector{String}}}()
        finfo = hdfs_get_path_info(fs, fname)
        blkdist = hdfs_get_hosts(fs, fname, 0, finfo.size) 
        d[source] = blkdist
        new(jid, fn_find_rec, fn_map, fn_collect, fn_reduce, HdfsJobSchedInfo(source, [source], d, length(blkdist)))
    end

    # constructor at the worker end
    function HdfsJobCtx(jid::JobId, rdr_typ::DataType, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        rdr = nothing
        (rdr_typ == HdfsReader) && (rdr = HdfsReader())
        (rdr_typ == MapResultReader) && (rdr = MapResultReader())
        new(jid, fn_find_rec, fn_map, fn_collect, fn_reduce, HdfsJobRunInfo(rdr,nothing))
    end
end

const _def_wrkr_job_store = Dict{JobId, HdfsJobCtx}()
const _job_store = Dict{JobId, HdfsJobCtx}()
const _jobpart_machines = Dict{Tuple,Vector{ASCIIString}}()
const _machine_jobparts = Dict{ASCIIString, Vector{Tuple}}()
const _procid_jobparts = Dict{Int, Vector{Tuple}}()

const STATE_ERROR = 0
const STATE_STARTING = 1
const STATE_RUNNING = 2
const STATE_COMPLETE = 3

# remove machines where workers are not running
# remap to other machines (currently remapping to "")
function __remap_macs_to_procs(macs)
    global _all_remote_names

    available_macs = filter(x->contains(_all_remote_names, x), macs)
    # TODO: figure out a better way to handle domain names
    if (length(available_macs) == 0) 
        macs = map(x->split(x,".")[1], macs)
        available_macs = filter(x->contains(_all_remote_names, x), macs)
    end
    (length(available_macs) == 0) && push!(available_macs, "")
    available_macs
end

function _distribute(jid::JobId, j::HdfsJobCtx, rdr_typ::DataType)
    (rdr_typ == HdfsReader) && (return _distribute_hdfs_file(jid, j))
    (rdr_typ == MapResultReader) && (return _distribute_map_result(jid, j))
end

function _distribute_map_result(jid::JobId, j::HdfsJobCtx)
    global _procid_jobparts

    sch::HdfsJobSchedInfo = j.info
    if(STATE_COMPLETE != wait(sch.source))
        _set_status(j, STATE_ERROR, "source has errors")
        return
    end
    for procid in 1:_num_remotes()
        t = (jid, string(sch.source))
        try
            push!(_procid_jobparts[mac], t)
        catch
            _procid_jobparts[procid] = [t]
        end
    end
end

function _distribute_hdfs_file(jid::JobId, j::HdfsJobCtx)
    global _jobpart_machines
    global _machine_jobparts

    sch::HdfsJobSchedInfo = j.info
    for fname in sch.source
        blk_dist = sch.dist_info[fname]
        for blk_id in 1:length(blk_dist)
            macs = blk_dist[blk_id]
            macs = __remap_macs_to_procs(macs)
            t = (jid, string(fname, '#', blk_id))
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

function _dequeue_job(jid::JobId)
    global _machine_jobparts
    global _procid_jobparts
    global _jobpart_machines

    for (k,v) in _machine_jobparts filter!(x->(jid != x[1]), v) end
    for (k,v) in _procid_jobparts filter!(x->(jid != x[1]), v) end

    filter!((k,v)->(jid != k[1]), _jobpart_machines)    
end

dmap(source::MRSource, fn_find_rec::Function, fn_map::Function, fn_collect::Function) = dmapreduce(source, fn_find_rec, fn_map, fn_collect, nothing)
function dmapreduce(source::MRSource, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
    global _remotes
    global _job_store
    global _debug

    (0 == length(_remotes)) && __prep_remotes()

    # create and set a job ctx
    jid = _next_job_id()
    j = HdfsJobCtx(jid, source, fn_find_rec, fn_map, fn_collect, fn_reduce)
    _job_store[jid] = j

    rdr_typ = isa(source, JobId) ? MapResultReader : HdfsReader
    @async begin
        try 
            # init job at all workers
            @sync begin
                for procid in 1:_num_remotes()
                    @async begin
                        try 
                            ret = remotecall_fetch(procid, HDFS._worker_init_job, jid, rdr_typ, fn_find_rec, fn_map, fn_collect, fn_reduce)
                            (ret != jid) && throw(ret)
                        catch ex
                            _debug && println("error initializing job $jid at $procid")
                            _set_status(j, STATE_ERROR, ex)
                        end
                    end
                end
            end

            # once created at all workers...
            _debug && println("distributing blocks for job $jid")
            _distribute(jid, j, rdr_typ)
            if(j.info.state != STATE_ERROR)
                _debug && println("distributed blocks for job $jid")
                _set_status(j, STATE_RUNNING)
                _sched()
                _debug && println("scheduled blocks for job $jid")
            end
        catch ex
            _debug && println("error starting job $jid")
            _set_status(j, STATE_ERROR, ex)
        end
    end
    jid
end

function wait(jid::JobId)
    global _job_store
    wait(_job_store[jid])
end
wait(j::HdfsJobCtx) = fetch(j.info.trigger)

function status(jid::JobId, desc::Bool=false)
    global _job_store
    status(_job_store[jid], desc)
end
function status(j::HdfsJobCtx, desc::Bool=false)
    function status_str()
        (j.info.state == STATE_RUNNING) && (return "running")
        (j.info.state == STATE_COMPLETE) && (return "complete")
        (j.info.state == STATE_STARTING) && (return "starting")
        (j.info.state == STATE_ERROR) && (return "error")
        error("job in unknown state")
    end

    !desc && return status_str()

    descinfo = j.info.state_info
    (j.info.state == STATE_RUNNING) && (descinfo = int(j.info.num_parts_done*100/j.info.num_parts))
    (status_str(), descinfo)
end

function _set_status(j::HdfsJobCtx, state::Int, desc=nothing)
    j.info.state = state
    (nothing != desc) && (j.info.state_info = desc)
    (STATE_ERROR == state) && _dequeue_job(j.jid)
    (STATE_RUNNING == state) && (j.info.sched_time = time())
    ((STATE_COMPLETE == state) || (STATE_ERROR == state)) && (j.info.end_time = time(); !isready(j.info.trigger) && put(j.info.trigger, state))
    state
end

function results(jid::JobId) 
    global _job_store
    results(_job_store[jid])
end
results(j::HdfsJobCtx) = (status(j), j.info.red)

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

function times(jid::JobId)
    global _job_store
    ji = _job_store[jid].info 
    t_total = (ji.end_time - ji.begin_time)
    t_sched = max(0, (ji.sched_time - ji.begin_time))
    t_run = (t_sched > 0) ? (ji.end_time - ji.sched_time) : 0
    (t_total, t_sched, t_run)
end

