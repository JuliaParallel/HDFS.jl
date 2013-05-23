
##
# Provides access to map results
type MapResultReader <: MapInputReader
    url::String
    jid::Int64
    results::Any

    function MapResultReader(url::String="")
        r = new("", 0, nothing)
        (length(url) > 0) && reset_pos(r, url)
        r
    end
end

function reset_pos(r::MapResultReader, url::String)
    if(url != r.url)
        # new source
        comps = urlparse(url)
        r.jid = int64(comps.url)
        j = ((myid() == 1) ? _def_wrkr_job_store : _job_store)[r.jid]
        r.results = j.info.results
        r.url = url
    end
end


##
# Input for map
type MRMapInput <: MRInput
    job_list::Vector{JobId}
    reader_fn::Function

    function MRMapInput(job_list, reader_fn::Function)
        jl = JobId[]
        for job_id in job_list
            push!(jl, convert(JobId, job_id))
        end
        new(jl, reader_fn)
    end
end


