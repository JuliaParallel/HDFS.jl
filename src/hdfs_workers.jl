using Base.Collections

import Base.Collections.dequeue!

function dequeue!(pq::PriorityQueue, key)
         !haskey(pq.index, key) && return
         idx = delete!(pq.index, key)
         splice!(pq.xs, idx)
         for (k,v) in pq.index
           (v >= idx) && (pq.index[k] = (v-1))
         end
     key
end


##n account of the worker machines available
const _remotes = Array(Vector{ASCIIString},0)
const _all_remote_names = Set{String}()
num_remotes() = length(_remotes[1])
function prep_remotes(force::Bool=false)
    !force && (0 != length(_remotes)) && return
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
#   - :wrkr_all, :wrkr_any
abstract WorkerTask
type QueuedWorkerTask
    wtask::WorkerTask
    remote_method::Function
    callback::FuncNone
    target::Union(Int,ASCIIString,Symbol,Vector{ASCIIString},Vector{Int})
    qtime::Float64
    function QueuedWorkerTask(wtask::WorkerTask, remote_method::Function, callback::FuncNone, target::Union(Int,ASCIIString,Symbol,Vector{ASCIIString},Vector{Int}))
        new(wtask, remote_method, callback, target, time())
    end
end


# Queues for distributing tasks
# TODO: use priorityqueues
const _machine_tasks = Dict{ASCIIString, PriorityQueue{QueuedWorkerTask,Float64}}()
const _procid_tasks = Dict{Int, PriorityQueue{QueuedWorkerTask, Float64}}()
const _any_tasks = PriorityQueue{QueuedWorkerTask,Float64}()

function queue_worker_task(t::QueuedWorkerTask) 
    _queue_worker_task(t, t.target)
    _start_feeders()
end
function _queue_worker_task(t::QueuedWorkerTask, procid::Int)
    !haskey(_procid_tasks, procid) && (_procid_tasks[procid] = PriorityQueue{QueuedWorkerTask,Float64}())
    (_procid_tasks[procid])[t] = Inf
end
_queue_worker_task(t::QueuedWorkerTask, procid_list::Vector{Int}) = for procid in procid_list _queue_worker_task(t, procid) end
function _queue_worker_task(t::QueuedWorkerTask, machine::ASCIIString)
    !haskey(_machine_tasks, machine) && (_machine_tasks[machine] = PriorityQueue{QueuedWorkerTask,Float64}())
    (_machine_tasks[machine])[t] = Inf
end
function _queue_worker_task(t::QueuedWorkerTask, machine_list::Vector{ASCIIString}) 
    function remap_macs_to_procs(macs)
        available_macs = filter(x->contains(_all_remote_names, x), macs)
        (length(available_macs) == 0) && (available_macs = filter(x->contains(_all_remote_names, x), map(x->split(x,".")[1], macs)))
        (length(available_macs) == 0) && push!(available_macs, "")
        available_macs
    end
    for machine in remap_macs_to_procs(machine_list) _queue_worker_task(t, machine) end
end
function _queue_worker_task(t::QueuedWorkerTask, s::Symbol)
    (:wrkr_all == s) && return _queue_worker_task(t::QueuedWorkerTask, [1:num_remotes()])
    (:wrkr_any == s) && return (_any_tasks[t] = Inf)
    error("unknown queue $(s)")
end

dequeue_worker_task(t::QueuedWorkerTask) = _dequeue_worker_task(t::QueuedWorkerTask, t.target)
_dequeue_worker_task(t::QueuedWorkerTask, procid::Int) = haskey(_procid_tasks, procid) && dequeue!(_procid_tasks[procid], t)
_dequeue_worker_task(t::QueuedWorkerTask, procid_list::Vector{Int}) = for procid in procid_list _dequeue_worker_task(t, procid) end
_dequeue_worker_task(t::QueuedWorkerTask, machine::ASCIIString) = haskey(_machine_tasks, machine) && dequeue!(_machine_tasks[machine], t)
_dequeue_worker_task(t::QueuedWorkerTask, machine_list::Vector{ASCIIString}) = for machine in machine_list _dequeue_worker_task(t, machine) end
function _dequeue_worker_task(t::QueuedWorkerTask, s::Symbol)
    (:wrkr_all == s) && return _dequeue_worker_task(t::QueuedWorkerTask, [1:num_remotes()])
    (:wrkr_any == s) && return filter!(x->(x != t), _any_tasks)
    error("unknown queue $(s)")
end
function dequeue_worker_task(filter_fn::Function)
    for (_,v) in _machine_tasks filter!(filter_fn, v) end
    for (_,v) in _procid_tasks filter!(filter_fn, v) end
    filter!(filter_fn, _any_tasks)
end

##
# scheduler function
function set_priorities(calc_prio::Function)
    for (k,v) in _machine_tasks for (k1,v1) in v v[k1] = calc_prio(k, k1, v1) end end
    for (k,v) in _procid_tasks for (k1,v1) in v v[k1] = calc_prio(k, k1, v1) end end
    for (k1, v1) in _any_tasks _any_tasks[k1] = calc_prio(:wrkr_any, k1, v1) end
end

const _feeders = Dict{Int,RemoteRef}()
function _start_feeders()
    _debug && println("starting feeders...")
    # start feeder tasks if required
    for procid in 1:num_remotes()
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

    qtask = dequeue!(v)
    (:wrkr_all != qtask.target) && dequeue_worker_task(qtask)
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
        ret = remotecall_fetch(procid, qtask.remote_method, wtask)
        _debug && println("returned from call to procid $(procid) for task $(wtask)")

        try
            (nothing != qtask.callback) && (qtask.callback(wtask, ret))
        catch ex
            _debug && println("error in callback $(ex)")
            _debug && println("returned value: $(ret)")
        end
    end
end


