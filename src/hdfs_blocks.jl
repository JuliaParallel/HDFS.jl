using Blocks
using Base.Order

import  Blocks.Block
export  Block

const loopbackip = IPv4(127,0,0,1)

function addloopback(block_dist)
    localip = getipaddr()
    for iplist in block_dist
        # add loopback interface for localip
        (localip in iplist) && !(loopbackip in iplist) && push!(iplist, loopbackip)
    end
    block_dist
end

function file_affinities(fs::HdfsFS, path::String, recurse::Bool, macprio::Base.Collections.PriorityQueue, list::Dict)
    localip = getipaddr()
    loopbackip = IPv4(127,0,0,1)

    file_infos = hdfs_list_directory(fs, path)
    for file_info in file_infos
        file_path = urlparse(file_info.name).url
        if file_info.kind == HDFS_OBJ_FILE
            # fetch blocks of the file and determine best possible hosts
            block_dist = addloopback(hdfs_blocks(fs, file_path))
            macs  = unique([block_dist...])
            for (m,p) in macprio
                if string(m) in macs
                    macprio[m] += 1
                    push!(list[m], file_info)
                    break
                end  
            end
        elseif (file_info.kind == HDFS_OBJ_DIR) && recurse
            file_affinities(fs, file_path, true, macprio, list)
        end
    end
    nothing
end

##
# Make blocks out of large number of small sized files in a HDFS directory.
# One can also specify:
#   - traverse sub folders
#   - the max number of files to place per block (0 for no limits)
function Block(f::HdfsURL, recurse::Bool=true, maxfiles::Int=0)
    macprio = Base.Collections.PriorityQueue()
    list = Dict()
    worker_ids = workers()
    localip = getipaddr()
    worker_ips = map(x->getaddrinfo(string(isa(x, LocalProcess)?localip:x.host)), map(x->Base.worker_from_id(x), worker_ids))
    for wip in worker_ips
        macprio[wip] = 0
        list[wip] = {}
    end

    comps = urlparse(f.url)
    path = comps.url
    fs = hdfs_connect(f)
    file_affinities(fs, path, recurse, macprio, list)

    data = {}
    affinities = {}
    for (wip, files) in list
        aff = worker_ids[findin(worker_ips, [wip])]
        while length(files) > 0
            push!(affinities, aff)
            if maxfiles > 0
                np = min(maxfiles,length(files))
                push!(data, map(x->HdfsURL(x.name), files[1:np]))
                files = files[(np+1):end]
            else
                push!(data, map(x->HdfsURL(x.name), files))
                break
            end
        end
    end
    Block([f], data, affinities, as_it_is, as_it_is)
end


function Block(f::HdfsURL)
    isdir(f) && (return Block(f, true, 0))

    worker_ids = workers()
    localip = getipaddr()
    worker_ips = map(x->getaddrinfo(string(isa(x, LocalProcess)?localip:x.host)), map(x->Base.worker_from_id(x), worker_ids))

    block_dist = addloopback(hdfs_blocks(f, 1, 0, true))
    block_wrkr_ids = map(ips->worker_ids[findin(worker_ips, ips)], block_dist)
    filestat = stat(f)
    block_sz = filestat.block_sz
    file_sz = filestat.size

    data = [(f, ((x-1)*block_sz+1):(min(file_sz,x*block_sz))) for x in 1:length(block_dist)]
    Block(f, data, block_wrkr_ids, as_it_is, as_it_is)
end

