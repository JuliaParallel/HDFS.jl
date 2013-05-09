##
# A bunch of methods that can be used to assist in map-reduce jobs

##
# generic routine to detect CSV type of records in a hdfs file block
# rec_sep: record separator character
# col_sep: column separator character
# max_rec_bytes: maximum possible bytes in a record as a hint. (used to read past the block to complete partial records at end of block)
# bchk: function that works on raw bytes to determine if this is interesting to be split into columns. used as an optimization for rchk
# rchk: function that works on a record (array of strings) to determine if this is interesting to be passed to process method
# read_beyond: flag used to recurse into the next block
function hdfs_find_rec_csv(jc::HdfsJobCtx, rec_sep, col_sep, max_rec_bytes::Int, bchk::Function=(x...)->true, rchk::Function=(x...)->true, read_beyond::Bool = true)
    ji = jc.info
    rdr = jc.info.rdr
    is_begin = (rdr.begin_blk == 1) # if first block, we should not ignore the first line
    final_pos = length(rdr.cv)
    end_pos = 0

    if(!is_begin)
        end_pos = search(rdr.cv, rec_sep, int(ji.next_rec_pos))
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, max_rec_bytes)
            return hdfs_find_rec_csv(jc, rec_sep, col_sep, 0, bchk, rchk, false)
        else
            ji.next_rec_pos = end_pos
        end
    end
  
    while(int64(ji.next_rec_pos) <= int64(final_pos))
        end_pos = search(rdr.cv, rec_sep, ji.next_rec_pos)-1
        #println("ji.next_rec_pos $ji.next_rec_pos, final_pos: $final_pos, end_pos: $end_pos, read_beyond: $read_beyond") 
        if((0 >= end_pos) && !eof(rdr) && read_beyond)
            read_next(rdr, max_rec_bytes)
            return hdfs_find_rec_csv(jc, rec_sep, col_sep, 0, bchk, rchk, false)
        else
            # if no rec boundary found, assume all data in buffer is the record.
            # this is valid only if this is the end of the file.
            # which should be true if max_rec_bytes is correct.
            # TODO: put a check and return error if not
            (0 >= end_pos) && (end_pos = final_pos)
            local is_interesting::Bool = bchk(jc)
            if(is_interesting)
                #ji.rec = split(ascii(rdr.cv[ji.next_rec_pos:end_pos]), col_sep)
                ji.rec = split(bytestring(sub(rdr.cv, int(ji.next_rec_pos):int(end_pos))), col_sep)
                is_interesting = rchk(jc)
            end
            ji.next_rec_pos = end_pos+2
            is_interesting && return :ok
        end
    end
    ji.next_rec_pos = final_pos+1
    ji.rec = []
    :not_ok
end

