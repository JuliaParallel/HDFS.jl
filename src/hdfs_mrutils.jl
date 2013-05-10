##
# A bunch of methods that can be used to assist in map-reduce jobs

##
# generic routine to detect CSV type of records in a hdfs file block
# rec_sep: record separator character
# col_sep: column separator character
# max_rec_bytes: maximum possible bytes in a record as a hint. (used to read past the block to complete partial records at end of block)
# tmplt: a template to match with. irrelevant columns can be nothing. performs an exact string match.
# read_beyond: flag used to recurse into the next block
function hdfs_find_rec_csv(jr::HdfsReaderIter, next_rec_pos, rec_sep, col_sep, max_rec_bytes::Int, tmplt::Tuple=(), read_beyond::Bool=true)
    rdr = jr.r
    is_begin = (rdr.begin_blk == 1) # if first block, we should not ignore the first line
    final_pos = length(rdr.cv)
    end_pos = 0
    begin_tmplt = ((length(tmplt) > 0) && (nothing != tmplt[1])) ? convert(Vector{Uint8}, tmplt[1]) : nothing

    if(!is_begin)
        end_pos = search(rdr.cv, rec_sep, int(next_rec_pos))
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, max_rec_bytes)
            return hdfs_find_rec_csv(jr, next_rec_pos, rec_sep, col_sep, 0, tmplt, false)
        else
            next_rec_pos = end_pos
        end
    end
  
    while(int64(next_rec_pos) <= int64(final_pos))
        end_pos = search(rdr.cv, rec_sep, next_rec_pos)-1
        #println("next_rec_pos $next_rec_pos, final_pos: $final_pos, end_pos: $end_pos, read_beyond: $read_beyond") 
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, max_rec_bytes)
            return hdfs_find_rec_csv(jr, next_rec_pos, rec_sep, col_sep, 0, tmplt, false)
        else
            # if no rec boundary found, assume all data in buffer is the record.
            # this is valid only if this is the end of the file.
            # which should be true if max_rec_bytes is correct.
            # TODO: put a check and return error if not
            (0 >= end_pos) && (end_pos = final_pos)
            is_interesting::Bool = (nothing != begin_tmplt) ? beginswithat(rdr.cv, int(next_rec_pos), begin_tmplt) : true
            if(is_interesting)
                jr.rec = split(bytestring(sub(rdr.cv, int(next_rec_pos):int(end_pos))), col_sep)
                col_idx = 0
                for tmplt_col in tmplt
                    col_idx += 1
                    (nothing == tmplt_col) && continue
                    !(is_interesting = (jr.rec[col_idx] == tmplt_col)) && break
                end
            end
            next_rec_pos = end_pos+2
            is_interesting && return next_rec_pos
        end
    end
    jr.rec = []
    next_rec_pos = final_pos+1
end

