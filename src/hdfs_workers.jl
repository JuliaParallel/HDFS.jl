
##n account of the worker machines available
const _remotes = Array(Vector{ASCIIString},0)
const _all_remote_names = Set{String}()
_num_remotes() = length(_remotes[1])
function __prep_remotes()
    empty!(_remotes)
    empty!(_all_remote_names)

    n = nprocs()
    ips = Array(ASCIIString, n-1)
    hns = Array(ASCIIString, n-1)
    for midx in 2:n
        ips[midx-1] = remotecall_fetch(midx, getipaddr)
        hns[midx-1] = remotecall_fetch(midx, gethostname)
    end

    for x in (ips, hns)
        push!(_remotes, x)
        union!(_all_remote_names, x)
        unshift!(x, "")     # to represent the default worker node
    end
end


# Worker task type. One of: map, file_info, reduce(in future)
# target can be:
#   - proc id, or a list of procids
#   - machine ip/hostname, or a list of ips/hostnames
#   - :all, :any
abstract WorkerTask
type QueuedWorkerTask
    wtask::WorkerTask
    callback::FuncNone
    target::Union(Int,ASCIIString,Symbol,Vector{ASCIIString},Vector{Int})
end


# Queues for distributing tasks
# TODO: use priorityqueues
const _machine_tasks = Dict{ASCIIString, Vector{QueuedWorkerTask}}()
const _procid_tasks = Dict{Int, Vector{QueuedWorkerTask}}()
const _any_tasks = Array(QueuedWorkerTask,0)

queue_worker_task(t::QueuedWorkerTask) = queue_worker_task(t, t.target)
queue_worker_task(t::QueuedWorkerTask, procid::Int) = haskey(_procid_tasks, procid) ? push!(_procid_tasks[procid], t) : (_procid_tasks[procid] = [t])
queue_worker_task(t::QueuedWorkerTask, procid_list::Vector{Int}) = for procid in procid_list queue_worker_task(t, procid) end
queue_worker_task(t::QueuedWorkerTask, machine::ASCIIString) = haskey(_machine_tasks, machine) ? push!(_machine_tasks[machine], t) : (_machine_tasks[machine] = [t])
queue_worker_task(t::QueuedWorkerTask, machine_list::Vector{ASCIIString}) = for machine in machine_list queue_worker_task(t, machine) end
function queue_worker_task(t::QueuedWorkerTask, s::Symbol)
    (:all == s) && return queue_worker_task(t::QueuedWorkerTask, 1:_num_remotes())
    (:any == s) && return push!(_any_tasks, t)
    error("unknown queue $(s)")
end

dequeue_worker_task(t::QueuedWorkerTask) = dequeue_worker_task(t::QueuedWorkerTask, t.target)
dequeue_worker_task(t::QueuedWorkerTask, procid::Int) = haskey(_procid_tasks, procid) && filter!(x->(x != t), _procid_tasks[procid])
dequeue_worker_task(t::QueuedWorkerTask, procid_list::Vector{Int}) = for procid in procid_list dequeue_worker_task(t, procid) end
dequeue_worker_task(t::QueuedWorkerTask, machine::ASCIIString) = haskey(_machine_tasks, machine) && filter!(x->(x != t), _machine_tasks[machine])
dequeue_worker_task(t::QueuedWorkerTask, machine_list::Vector{ASCIIString}) = for machine in machine_list dequeue_worker_task(t, machine) end
function dequeue_worker_task(t::QueuedWorkerTask, s::Symbol)
    (:all == s) && return dequeue_worker_task(t::QueuedWorkerTask, 1:_num_remotes())
    (:any == s) && return filter!(x->(x != t), _any_tasks)
    error("unknown queue $(s)")
end
function dequeue_worker_task(filter_fn::Function)
    for (k,v) in _machine_tasks filter!(filter_fn, v) end
    for (k,v) in _procid_tasks filter!(filter_fn, v) end
    filter!(filter_fn, _any_tasks)
end

##
# scheduler function
const _feeders = Dict{Int,RemoteRef}()
function start_feeders()
    _debug && println("starting feeders...")
    # start feeder tasks if required
    for procid in 1:_num_remotes()
        haskey(_feeders, procid) && !isready(_feeders[procid]) && continue
        ip, hn = map(x->x[procid], _remotes)
        (nothing == _fetch_tasks(procid, ip, hn, true)) && continue
        _debug && println("\tstarting feeder for $(procid)")
        _feeders[procid] = @async _push_to_worker(procid)
    end
end

function _fetch_tasks(proc_id::Int, ip::String, hn::String, peek::Bool=false)
    v = _any_tasks
    ((v == nothing) || (0 == length(v))) && haskey(_procid_tasks, proc_id) && (v = _procid_tasks[proc_id])
    ((v == nothing) || (0 == length(v))) && haskey(_machine_tasks, ip) && (v = _machine_tasks[ip])
    ((v == nothing) || (0 == length(v))) && haskey(_machine_tasks, hn) && (v = _machine_tasks[hn])
    ((v == nothing) || (0 == length(v))) && return nothing

    peek && (return ((length(v) > 0) ? length(v) : nothing))

    qtask = shift!(v)
    dequeue_worker_task(qtask)
    qtask
end


function _push_to_worker(procid)
    ip, hn = map(x->x[procid], _remotes)

    while(true)
        # if no tasks, exit task
        qtask = _fetch_tasks(procid, ip, hn)
        (qtask == nothing) && return

        wtask = qtask.wtask

        # call method at worker
        _debug && println("calling procid $(procid) for task $(wtask)")
        ret = remotecall_fetch(procid, HDFS._worker_task, wtask)
        _debug && println("returned from call to procid $(procid) for task $(wtask)")

        try
            (nothing != qtask.callback) && (qtask.callback(wtask, ret))
        catch ex
            _debug && println("error in callback $(ex)")
            _debug && println("returned value: $(ret)")
        end
    end
end


