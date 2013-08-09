using DataFrames
using HDFS
using Blocks
using Blocks.DDataFrames

function find_nrows(datafile)
    b = Block(datafile)
    b |> as_recordio |> as_bufferedio |> x->as_dataframe(x, separator='\t');

    pmapreduce(nrow, +, b)
end

function filter_smiley(df, s)
    (df[:(x4 .== ":)"), :])[:x3]
end

function find_smiley_counts(datafile)
    b = Block(datafile)
    b |> as_recordio |> as_bufferedio
    dt = dreadtable(b, separator='\t')
    by(dt, [:x4], x->sum(x[:x3]), sum)
end

function run_tests()
    datafile = HdfsURL("hdfs://localhost:9000/user/tan/streaminp/smileys_for_julia.tsv")
    nrows = find_nrows(datafile)
    smiley_counts = find_smiley_counts(datafile)

    println("num rows: $nrows")
    println(smiley_counts)
end

