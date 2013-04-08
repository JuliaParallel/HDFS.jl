HDFS.jl
=======

HDFS interface for Julia as a wrapper over Hadoop HDFS library.
Derived from JuliaLang/HDFS.jl.

Some of the methods (those that depend on system dependent C stuff like time\_t and fcntl flags) may not have been ported to all systems yet.

TODO:
- a way to generate system dependent constants
- convenience julia macros/methods to make working with HDFS intuitive.

