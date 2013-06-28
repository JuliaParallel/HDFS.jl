##
# A bunch of methods that can be used to assist in map-reduce jobs

## 
# generic reduce functions
function reduce_dicts(op::Function, result, results...)
    for d in results
        (nothing == d) && continue
        (nothing == result) && (result = similar(d))
        for (k,v) in d
            haskey(result, k) ? op(result[k],v) : (result[k] = v)
        end
    end
    result
end

##
# generic collect functions 
collect_in_vector(results, rec) = (nothing == results) ? [rec] : push!(results, rec)
collect_in_set(results, rec) = (nothing == results) ? Set(rec) : add!(results, rec)
function collect_in_dict(op::Function, results, rec)
    (nothing == rec) && return results
    k,v = rec
    if(nothing == results) 
        results = Dict{typeof(k),typeof(v)}()
        results[k] = v
        return results
    end
    !haskey(results, k) && (results[k]=v; return results)
    results[k] = op(v,results[k])
    results
end


##
# generic routine to filter records from map results that of list type
# 
function find_rec(r::MapResultReader, iter_status, filter_fn::FuncNone=nothing)
    results = r.results
    ret_rec = nothing
    (nothing == results) && (return (ret_rec, true, iter_status))
    (nothing == iter_status) && (iter_status = start(results))

    while(!done(results, iter_status))
        ret_rec, iter_status = next(results, iter_status)
        ((nothing == filter_fn) || filter_fn(ret_rec)) && return (ret_rec, false, iter_status)
    end
    (nothing, true, iter_status)
end


##
# make a dataframe out of the whole block and pass that as a record to map
function find_rec(rdr::HdfsBlockReader, iter_status, t::Type{Matrix}, rec_sep::Char='\n', col_sep::Char=',')
    (iter_status != nothing) && return (nothing, true, iter_status)
    ios = get_stream(rdr)
    dlmarr = readdlm(ios, col_sep, rec_sep)
    (dlmarr, false, position(ios))
end

##
# make a dataframe out of the whole block and pass that as a record to map
function find_rec(rdr::HdfsBlockReader, iter_status, t::Type{DataFrame}, rec_sep::Char='\n', col_sep::Char=',')
    #(iter_status != nothing) && return (nothing, true, iter_status)
    #ios = get_stream(rdr)
    #sz = nb_available(ios)
    #println("bytes available: $sz")
    #df = readtable(ios, header=false, separator=col_sep, nrows=sz, buffersize=sz)
    #return (df, false, position(ios))

    (dlmarr, iseof, iter_status) = find_rec(rdr, iter_status, Matrix, rec_sep, col_sep)
    df = (nothing == dlmarr) ? nothing : DataFrame(dlmarr)
    #println("hdfs_find_rec_table got $dlmarr")
    (df, iseof, iter_status)
end

function find_rec(rdr::HdfsBlockReader, iter_status, t::Type{Vector}, rec_sep::Char='\n', col_sep::Char=',', filters::Tuple=())
    ios = get_stream(rdr)
    eof(ios) && return(nothing, true, iter_status)
    # TODO: use rec_sep, split into substrings, coltype support
    found = false
    splt = []
    while !found
        l = rstrip(readline(ios))
        !isempty(filters) && isa(filters[1], String) && !beginswith(l, filters[1]) && continue
        splt = split(l, col_sep)
        isempty(filters) && return splt
        found = true
        for idx in 1:length(filters)
            f = filters[idx]
            (nothing == f) && continue
            isa(f, Regex) && ismatch(splt[idx], f) && continue
            isa(f, String) && (f == splt[idx]) && continue
            isa(f, Function) && f(splt[idx]) && continue
            found = false
            break
        end
    end
    (splt, false, position(ios))
end

