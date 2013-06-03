
##
# Assign Job Ids. A one up number
global __next_job_id = int64(0)::JobId
function _next_job_id()
    global __next_job_id
    __next_job_id += 1
end


##
# Worker task type. One of: map, file_info, reduce(in future)
type WorkerTaskMap <: WorkerTask
    jid::JobId
    blk_url::String
end

type WorkerTaskFileInfo <: WorkerTask
    jid::JobId
    source::MRFileInput
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
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid] = HdfsJobCtx(jid, inp_typ, fn_find_rec, fn_map, fn_collect, fn_reduce)
    jid
end

function _worker_unload(jid::JobId)
    d = ((myid() == 1) ? _def_wrkr_job_store : _job_store)
    delete!(d, jid)
    jid
end

function _worker_fetch_collected(jid::JobId) 
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[jid].info.results
end

function _worker_task(t::WorkerTaskMap)
    j = ((myid() == 1) ? _def_wrkr_job_store : _job_store)[t.jid]
    jinfo = j.info
    recs = 0
    fn_map = j.fn_map
    fn_collect = j.fn_collect

    results = jinfo.results
    for rec in MapInputIterator(jinfo.rdr, t.blk_url, j.fn_find_rec)
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

function _worker_task(t::WorkerTaskFileInfo)
    expand_file_inputs(t.source)
end


##
# scheduler function
function _sched()
    # TODO: recalculate priorities and reorder _machine_jobparts
    start_feeders()
end

function _callback(t::WorkerTaskFileInfo, ret)
    # remove machines where workers are not running
    # remap to other machines. map to default node "" if not running on any of the nodes
    function remap_macs_to_procs(macs)
        global _all_remote_names

        available_macs = filter(x->contains(_all_remote_names, x), macs)
        (length(available_macs) == 0) && (available_macs = filter(x->contains(_all_remote_names, x), map(x->split(x,".")[1], macs)))
        (length(available_macs) == 0) && push!(available_macs, "")
        available_macs
    end

    jid = t.jid
    try
        !isa(ret, MRFileInput) && throw(ret)

        nparts = 0
        for (idx,fname) in enumerate(ret.file_list)
            blk_dist = ret.file_blocks[idx]
            nparts += length(blk_dist)
            for (blk_id,macs) in enumerate(blk_dist)
                macs = remap_macs_to_procs(macs)
                queue_worker_task(QueuedWorkerTask(WorkerTaskMap(jid, string(fname, '#', blk_id)), HDFS._callback, macs))
            end
        end
        _start_running(jid, nparts)
    catch ex
        _debug && println("error starting job $(jid)")
        _set_status(j, STATE_ERROR, ex)
    end
end

function _callback(t::WorkerTaskMap, ret)
    jid = t.jid
    j = _job_store[jid] 
    (j.info.state == STATE_ERROR) && return

    if(!isa(ret, Integer))
        _set_status(j, STATE_ERROR, ret)
    else
        _debug && println("num records mapped: $(ret)")
        j.info.num_parts_done += 1
    end

    (j.info.state != STATE_ERROR) && (j.info.num_parts == j.info.num_parts_done) && _reduce_from_workers(j)
end

function _reduce_from_workers(j)
    try
        if(j.fn_reduce != nothing)
            @sync begin
                # TODO: distributed reduction and not plugged in at this place
                for procid in 1:_num_remotes()
                    @async begin
                        red = remotecall_fetch(procid, HDFS._worker_fetch_collected, j.jid)
                        isa(red,Exception) && throw(red)
                        j.info.red = j.fn_reduce(j.info.red, red)
                    end
                end
            end
        end
        _set_status(j, STATE_COMPLETE)
    catch ex
        _set_status(j, STATE_ERROR, ex)
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

const STATE_ERROR = 0
const STATE_STARTING = 1
const STATE_RUNNING = 2
const STATE_COMPLETE = 3

function _start_running(jid::JobId, nparts)
    j = _job_store[jid]
    j.info.num_parts = nparts
    if(j.info.state != STATE_ERROR)
        _debug && println("distributed blocks for job $jid")
        _set_status(j, STATE_RUNNING)
        _sched()
        _debug && println("scheduled blocks for job $jid")
    end
end

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
        nparts += _num_remotes()
        queue_worker_task(QueuedWorkerTask(WorkerTaskMap(jid, string(src_jid)), HDFS._callback, :all))
    end
    _start_running(jid, nparts)
end
function _distribute(jid::JobId, source::MRFileInput) 
    queue_worker_task(QueuedWorkerTask(WorkerTaskFileInfo(jid,source), HDFS._callback, :any))
    start_feeders()
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
            _distribute(jid, source)
            #j.info.num_parts = _distribute(jid, source)
            #if(j.info.state != STATE_ERROR)
            #    _debug && println("distributed blocks for job $jid")
            #    _set_status(j, STATE_RUNNING)
            #    _sched()
            #    _debug && println("scheduled blocks for job $jid")
            #end
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
    (STATE_ERROR == state) && dequeue_worker_task((x)->(!isa(x.wtask, WorkerTaskMap) || (j.jid != x.wtask.jid))) 
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

function start_workers(remotefile::String; tunnel=false, dir=JULIA_HOME, exename="./julia-release-basic")
    machines = split(readall(remotefile), '\n', false)
    addprocs(machines, tunnel=tunnel, dir=dir, exename=exename)
end

