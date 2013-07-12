
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
    source::MRHdfsFileInput
end

type WorkerTaskInitJob <:WorkerTask
    jid::JobId
    inp_typ::Any
    fn_find_rec::Function
    fn_map::Function
    fn_collect::Function
    fn_reduce::FuncNone
end

type WorkerTaskUnloadJob <:WorkerTask
    jid::JobId
end

type WorkerTaskFetchCollected <:WorkerTask
    jid::JobId
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


function _worker_task(t::WorkerTaskInitJob)
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[t.jid] = HdfsJobCtx(t.jid, t.inp_typ, t.fn_find_rec, t.fn_map, t.fn_collect, t.fn_reduce)
    t.jid
end

function _worker_task(t::WorkerTaskUnloadJob)
    d = ((myid() == 1) ? _def_wrkr_job_store : _job_store)
    j = delete!(d, t.jid)
    isa(j.info, HdfsJobRunInfo) && isa(j.info.rdr, MapStreamInputReader) && close(j.info.rdr)
    t.jid
end

function _worker_task(t::WorkerTaskFetchCollected)
    ((myid() == 1) ? _def_wrkr_job_store : _job_store)[t.jid].info.results
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
# TODO: queued task priorities to be linked to the chain
function _sched()
    set_priorities((_1,qt,_2)->float64(qt.wtask.jid))
end

function _callback(t::WorkerTaskInitJob, ret)
    jid = t.jid
    !haskey(_job_store, jid) && return # if job is deleted because of a past error
    j = _job_store[jid]

    (ret != jid) && return _set_status(j, STATE_ERROR, ret)
    if(0 == (j.info.num_pending_inits -= 1))
        _debug && println("distributing blocks for job $jid")
        @async begin
            try
                _distribute(jid, j.info.source_spec)
            catch ex
                _debug && println("error distributing job $jid")
                _set_status(j, STATE_ERROR, ex)
            end
        end
    end
end

function _callback(t::WorkerTaskFetchCollected, ret)
    jid = t.jid
    !haskey(_job_store, jid) && return # if job is deleted because of a past error
    j = _job_store[jid]

    if(isa(ret,Exception))
        _set_status(j, STATE_ERROR, ret)
        dequeue_worker_task(t)
        return
    end
    ji = j.info
    try
        ji.red = j.fn_reduce(ji.red, ret)
        ji.num_reduces_done += 1
        (ji.num_reduces == ji.num_reduces_done) && _set_status(j, STATE_COMPLETE)
    catch ex
        _debug && println("error reducing job $jid")
        _set_status(j, STATE_ERROR, ex)
        return
    end
end

function _callback(t::WorkerTaskFileInfo, ret)
    jid = t.jid
    j = _job_store[jid] 
    try
        !isa(ret, MRHdfsFileInput) && throw(ret)

        nparts = 0
        for (idx,fname) in enumerate(ret.file_list)
            blk_dist = ret.file_blocks[idx]
            nparts += length(blk_dist)
            for (blk_id,macs) in enumerate(blk_dist)
                queue_worker_task(QueuedWorkerTask(WorkerTaskMap(jid, string(fname, '#', blk_id)), HDFS.MapReduce._worker_task, HDFS.MapReduce._callback, macs))
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
    !isa(ret, Integer) && (return _set_status(j, STATE_ERROR, ret))

    _debug && println("num records mapped: $(ret)")
    j.info.num_parts_done += 1

    if(j.info.num_parts == j.info.num_parts_done)
        (j.fn_reduce == nothing) && (return _set_status(j, STATE_COMPLETE))
        j.info.num_reduces = num_remotes()
        queue_worker_task(QueuedWorkerTask(WorkerTaskFetchCollected(jid), HDFS.MapReduce._worker_task, HDFS.MapReduce._callback, :wrkr_all))
    end
end


##
# Job store and scheduler
type HdfsJobSchedInfo
    source_spec::MRInput                                    # job_id or url (optionally with wildcard)
    num_parts::Int
    num_parts_done::Int
    num_pending_inits::Int
    num_reduces::Int
    num_reduces_done::Int
    state::Int
    state_info::Any
    red::Any
    trigger::RemoteRef
    begin_time::Float64                                     # submitted
    sched_time::Float64                                     # scheduled (after dependent jobs finish)
    end_time::Float64                                       # completed

    function HdfsJobSchedInfo(source_spec::MRInput)
        new(source_spec, 0, 0, 0, 0, 0, STATE_STARTING, nothing, nothing, RemoteRef(), time(), 0.0, 0.0)
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
    function HdfsJobCtx(jid::JobId, inp_typ, fn_find_rec::Function, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
        rdr = get_input_reader(inp_typ...)
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
    qtarr = map(x->QueuedWorkerTask(WorkerTaskMap(jid, string(x)), HDFS.MapReduce._worker_task, HDFS.MapReduce._callback, :wrkr_all), source.job_list)
    for src_jid in source.job_list
        if(STATE_COMPLETE != wait(src_jid))
            j = _job_store[jid]
            _set_status(j, STATE_ERROR, "source job id $(src_jid) has errors")
            return
        end
    end
    nparts = length(qtarr) * num_remotes()
    for qt in qtarr queue_worker_task(qt) end
    _start_running(jid, nparts)
end
function _distribute(jid::JobId, source::MRHdfsFileInput) 
    queue_worker_task(QueuedWorkerTask(WorkerTaskFileInfo(jid,source), HDFS.MapReduce._worker_task, HDFS.MapReduce._callback, :wrkr_any))
end
function _distribute(jid::JobId, source::MRFsFileInput)
    j = _job_store[jid] 
    try
        expand_file_inputs(source)
        nparts = 0
        for (idx,fname) in enumerate(source.file_list)
            nfileblks = int(ceil(source.file_info[idx].size/source.block_sz))
            nparts += nfileblks
            for blk_id in 1:nfileblks
                queue_worker_task(QueuedWorkerTask(WorkerTaskMap(jid, string(fname, '#', blk_id)), HDFS.MapReduce._worker_task, HDFS.MapReduce._callback, :wrkr_any))
            end
        end
        _start_running(jid, nparts)
    catch ex
        _debug && println("error starting job $(jid)")
        _set_status(j, STATE_ERROR, ex)
    end
end

dmap(source::MRInput, fn_map::Function, fn_collect::Function) = dmapreduce(source, fn_map, fn_collect, nothing)
function dmapreduce(source::MRInput, fn_map::Function, fn_collect::Function, fn_reduce::FuncNone)
    prep_remotes()

    jid = _next_job_id()
    j = HdfsJobCtx(jid, source, fn_map, fn_collect, fn_reduce)
    _job_store[jid] = j

    j.info.num_pending_inits = num_remotes()
    queue_worker_task(QueuedWorkerTask(WorkerTaskInitJob(jid, input_reader_type(source), source.reader_fn, fn_map, fn_collect, fn_reduce), HDFS.MapReduce._worker_task, HDFS.MapReduce._callback, :wrkr_all))
    jid
end

wait(jid::JobId) = wait(_job_store[jid])
wait(j::HdfsJobCtx) = fetch(j.info.trigger)

status(jid::JobId, desc::Bool=false) = status(_job_store[jid], desc)
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

results(jid::JobId) = results(_job_store[jid])
results(j::HdfsJobCtx) = (status(j), j.info.red)

function unload(jid::JobId)
    j = _job_store[jid]
    (j.info.state == STATE_RUNNING) && error("can't unload running job")
    queue_worker_task(QueuedWorkerTask(WorkerTaskUnloadJob(jid), HDFS.MapReduce._worker_task, nothing, :wrkr_all))
    delete!(_job_store, jid)
    jid
end 

function times(jid::JobId)
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

