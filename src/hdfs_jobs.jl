
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
# Iterator for MapInputReader using the find_rec function
type MapInputIterator
    r::MapInputReader
    fn_find_rec::Function
    is_done::Bool
    rec::Any

    function MapInputIterator(r::MapInputReader, url::String, fn_find_rec::Function)
        reset_pos(r, url)
        new(r, fn_find_rec, false, nothing)
    end
end

function start(iter::MapInputIterator) 
    iter.rec, iter.is_done, state = iter.fn_find_rec(iter.r, nothing)
    state
end
function next(iter::MapInputIterator, state)
    ret_rec = iter.rec
    iter.rec, iter.is_done, state = iter.fn_find_rec(iter.r, state)
    (ret_rec, state)
end
done(iter::MapInputIterator, state) = iter.is_done



##
# Methods used only at worker nodes
function _worker_init_job(jid::JobId, inp_typ::DataType, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
    global _job_store
    global _def_wrkr_job_store
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid] = HdfsJobCtx(jid, inp_typ, fn_find_rec, fn_map, fn_collect, fn_reduce)
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
    for rec in MapInputIterator(jinfo.rdr, chunk_url, j.fn_find_rec)
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
    source_spec::MRInput                                    # job_id or url (optionally with wildcard)
    num_parts::Int
    num_parts_done::Int
    state::Int
    state_info::Any
    red::Any
    trigger::RemoteRef
    begin_time::Float64                                     # submitted
    sched_time::Float64                                     # scheduled (after dependent jobs finish)
    end_time::Float64                                       # completed

    function HdfsJobSchedInfo(source_spec::MRInput)
        new(source_spec, 0, 0, STATE_STARTING, nothing, nothing, RemoteRef(), time(), 0.0, 0.0)
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
    function HdfsJobCtx(jid::JobId, source::MRInput, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        new(jid, source.reader_fn, fn_map, fn_collect, fn_reduce, HdfsJobSchedInfo(source))
    end

    # constructor at the worker end
    function HdfsJobCtx(jid::JobId, inp_typ::DataType, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        rdr = nothing
        (inp_typ == MRFileInput) && (rdr = HdfsReader())
        (inp_typ == MRMapInput) && (rdr = MapResultReader())
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

function _distribute(jid::JobId, source::MRMapInput)
    global _procid_jobparts

    for src_jid in source.job_list
        if(STATE_COMPLETE != wait(src_jid))
            _set_status(j, STATE_ERROR, "source job id $(src_jid) has errors")
            return
        end
    end
    nparts = 0
    for src_jid in source.job_list
        t = (jid, string(src_jid))
        nparts += _num_remotes()
        for procid in 1:_num_remotes()
            haskey(_procid_jobparts, procid) ? push!(_procid_jobparts[procid], t) : (_procid_jobparts[procid] = [t])
        end
    end
    nparts
end

function _distribute(jid::JobId, source::MRFileInput)
    global _jobpart_machines
    global _machine_jobparts

    function get_blks(url)
        up = urlparse(url)
        uname = username(up)
        fname = up.url
        fs = hdfs_connect(hostname(up), port(up), (nothing == uname) ? "" : uname)
        finfo = hdfs_get_path_info(fs, fname)
        return hdfs_blocks(fs, fname, 0, finfo.size) 
    end

    # remove machines where workers are not running
    # remap to other machines. map to default node "" if not running on any of the nodes
    # TODO: since julia and hadoop may report different hostnames, is there a good way to get all hostnames of a node?
    function remap_macs_to_procs(macs)
        global _all_remote_names

        available_macs = filter(x->contains(_all_remote_names, x), macs)
        (length(available_macs) == 0) && (available_macs = filter(x->contains(_all_remote_names, x), map(x->split(x,".")[1], macs)))
        (length(available_macs) == 0) && push!(available_macs, "")
        available_macs
    end

    nparts = 0
    for fname in source.file_list
        blk_dist = get_blks(fname)
        nparts += length(blk_dist)
        for (blk_id,macs) in enumerate(blk_dist)
            macs = remap_macs_to_procs(macs)
            t = (jid, string(fname, '#', blk_id))
            _jobpart_machines[t] = macs
            for mac in macs
                # TODO: notmalize hostnames to ips
                haskey(_machine_jobparts, mac) ?  push!(_machine_jobparts[mac], t) : (_machine_jobparts[mac] = [t])
            end
        end
    end
    nparts
end

function _dequeue_job(jid::JobId)
    global _machine_jobparts
    global _procid_jobparts
    global _jobpart_machines

    for (k,v) in _machine_jobparts filter!(x->(jid != x[1]), v) end
    for (k,v) in _procid_jobparts filter!(x->(jid != x[1]), v) end

    filter!((k,v)->(jid != k[1]), _jobpart_machines)    
end

dmap(source::MRInput, fn_map::Function, fn_collect::Function) = dmapreduce(source, fn_map, fn_collect, nothing)
function dmapreduce(source::MRInput, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
    global _remotes
    global _job_store
    global _debug

    (0 == length(_remotes)) && __prep_remotes()

    # create and set a job ctx
    jid = _next_job_id()
    j = HdfsJobCtx(jid, source, fn_map, fn_collect, fn_reduce)
    _job_store[jid] = j

    inp_typ = typeof(source)
    fn_find_rec = source.reader_fn
    @async begin
        try 
            # init job at all workers
            @sync begin
                for procid in 1:_num_remotes()
                    @async begin
                        try 
                            ret = remotecall_fetch(procid, HDFS._worker_init_job, jid, inp_typ, fn_find_rec, fn_map, fn_collect, fn_reduce)
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
            j.info.num_parts = _distribute(jid, source)
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
    ji = j.info
    function status_str()
        (ji.state == STATE_RUNNING) && (return "running")
        (ji.state == STATE_COMPLETE) && (return "complete")
        (ji.state == STATE_STARTING) && (return "starting")
        (ji.state == STATE_ERROR) && (return "error")
        error("job in unknown state")
    end

    !desc && return status_str()

    descinfo = ji.state_info
    (ji.state == STATE_RUNNING) && (descinfo = (0 == ji.num_parts) ? 0 : int(ji.num_parts_done*100/ji.num_parts))
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

