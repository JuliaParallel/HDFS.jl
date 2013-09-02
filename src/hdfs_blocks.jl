using Blocks

import  Blocks.Block

export  Block

function Block(f::HdfsURL)
    worker_ids = workers()
    localip = getipaddr()
    loopbackip = IPv4(127,0,0,1)
    worker_ips = map(x->getaddrinfo(string(isa(x, LocalProcess)?localip:x.host)), map(x->Base.worker_from_id(x), worker_ids))

    block_dist = hdfs_blocks(f, 1, 0, true)
    for iplist in block_dist
        # add loopback interface for localip
        (localip in iplist) && !(loopbackip in iplist) && push!(iplist, loopbackip)
    end
    block_wrkr_ids = map(ips->worker_ids[findin(worker_ips, ips)], block_dist)
    filestat = stat(f)
    block_sz = filestat.block_sz
    file_sz = filestat.size

    data = [(f, ((x-1)*block_sz+1):(min(file_sz,x*block_sz))) for x in 1:length(block_dist)]
    Block(f, data, block_wrkr_ids, as_it_is, as_it_is)
end

