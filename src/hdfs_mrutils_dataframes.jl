using DataFrames

##
# make a dataframe out of the whole block and pass that as a record to map
function find_rec(rdr::MapStreamInputReader, iter_status, t::Type{DataFrame}, rec_sep::Char='\n', col_sep::Char=',')
    (iter_status != nothing) && return (nothing, true, iter_status)
    ios = get_stream(rdr)
    sz = nb_available(ios)
    # frequent peek done by DataFrame reader causes libhdfs to misbehave
    buff = Array(Uint8, sz)
    read(ios, buff)
    pb = PipeBuffer(buff)
    #println("bytes available: $sz")
    df = readtable(pb, header=false, separator=col_sep, nrows=sz, buffersize=sz)
    return (df, false, position(ios))

    #(dlmarr, iseof, iter_status) = find_rec(rdr, iter_status, Matrix, rec_sep, col_sep)
    #df = (nothing == dlmarr) ? nothing : DataFrame(dlmarr)
    #println("hdfs_find_rec_table got $dlmarr")
    #(df, iseof, iter_status)
end

