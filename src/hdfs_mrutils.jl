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
function mr_result_find_rec(r::MapResultReader, iter_status, filter_fn::FuncNone=nothing)
    results = r.results
    ret_rec = nothing
    (nothing == results) && (return (ret_rec, true, iter_status))
    (nothing == iter_status) && (iter_status = start(results))

    while(!done(results, iter_status))
        ret_rec, iter_status = next(results, iter_status)
        ((nothing == filter_fn) || filter_fn(ret_rec)) && return (ret_rec, false, iter_status)
    end
    return (nothing, true, iter_status)
end

##
# generic routine to detect CSV type of records in a hdfs file block
# rec_sep: record separator character
# col_sep: column separator character
# max_rec_bytes: maximum possible bytes in a record as a hint. (used to read past the block to complete partial records at end of block)
# tmplt: a template to match with. irrelevant columns can be nothing. performs an exact string match.
# read_beyond: flag used to recurse into the next block
function hdfs_find_rec_csv(rdr::HdfsReader, iter_status, rec_sep, col_sep, max_rec_bytes::Int, tmplt::Tuple=(), read_beyond::Bool=true)
    is_begin = (rdr.begin_blk == 1) # if first block, we should not ignore the first line
    final_pos = length(rdr.cv)
    end_pos = 0
    begin_tmplt = ((length(tmplt) > 0) && (nothing != tmplt[1])) ? convert(Vector{Uint8}, tmplt[1]) : nothing
    next_rec_pos = (nothing == iter_status) ? 1 : iter_status
    ret_rec = []

    if(!is_begin)
        end_pos = search(rdr.cv, rec_sep, int(next_rec_pos))
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, max_rec_bytes)
            return hdfs_find_rec_csv(rdr, next_rec_pos, rec_sep, col_sep, 0, tmplt, false)
        else
            next_rec_pos = end_pos
        end
    end
  
    while(int64(next_rec_pos) <= int64(final_pos))
        end_pos = search(rdr.cv, rec_sep, next_rec_pos)-1
        #println("next_rec_pos $next_rec_pos, final_pos: $final_pos, end_pos: $end_pos, read_beyond: $read_beyond") 
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, max_rec_bytes)
            return hdfs_find_rec_csv(rdr, next_rec_pos, rec_sep, col_sep, 0, tmplt, false)
        else
            # if no rec boundary found, assume all data in buffer is the record.
            # this is valid only if this is the end of the file.
            # which should be true if max_rec_bytes is correct.
            # TODO: put a check and return error if not
            (0 >= end_pos) && (end_pos = final_pos)
            is_interesting::Bool = (nothing != begin_tmplt) ? beginswithat(rdr.cv, int(next_rec_pos), begin_tmplt) : true
            if(is_interesting)
                ret_rec = split(bytestring(sub(rdr.cv, int(next_rec_pos):int(end_pos))), col_sep)
                col_idx = 0
                for tmplt_col in tmplt
                    col_idx += 1
                    (nothing == tmplt_col) && continue
                    !(is_interesting = (ret_rec[col_idx] == tmplt_col)) && break
                end
            end
            next_rec_pos = end_pos+2
            is_interesting && return (ret_rec, (next_rec_pos > block_sz(rdr)), next_rec_pos)
        end
    end
    return (nothing, true, final_pos+1)                      # next rec pos is beyond final pos to indicating end
end

