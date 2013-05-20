
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
# Iterator for HdfsReader using the find_rec function
type MapResultReaderIter <: MapInputIterator
    r::MapResultReader
    fn_find_rec::Function
    is_done::Bool
    rec::Union(Any,Nothing)
end

function iterator(r::MapResultReader, url::String, fn_find_rec::Function)
    reset_pos(r, url)
    MapResultReaderIter(r, fn_find_rec, false, nothing)
end

start(iter::MapResultReaderIter) = iter.fn_find_rec(iter, nothing)
done(iter::MapResultReaderIter, state) = iter.is_done 
next(iter::MapResultReaderIter, state) = (iter.rec, iter.fn_find_rec(iter, state))

