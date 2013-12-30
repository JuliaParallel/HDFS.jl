## Installation

- Use `Pkg.add("HDFS")` to install the HDFS.jl module.
- Ensure the `libhdfs` shared library is built and can be loaded. The `libhdfs` APIs have changed in Hadoop 2.x versions and will not possibly work with HDFS.jl yet. However any Hadoop 1.x versions should work fine.
- Ensure a `JVM` is installed and its shared libraries can be loaded by `libhdfs`.
- Ensure `libhdfs` and `JVM` dynamic libraries are loadable. They should either be available in standard library path (e.g. `/usr/lib`, `/usr/local/lib`) or their locations be added to appropriate environment variables (e.g. `DYLD_LIBRARY_PATH`, `LD_LIBRARY_PATH`)
- Add hadoop Java libraries to `CLASSPATH`. E.g.
    ````
    HADOOP_HOME=[installation folder]/hadoop-1.2.1
    CLASSPATH1=$(JARS=("$HADOOP_HOME"/lib/*.jar); IFS=:; echo "${JARS[*]}")
    CLASSPATH2=$(JARS=("$HADOOP_HOME"/*.jar); IFS=:; echo "${JARS[*]}")
    export CLASSPATH=${CLASSPATH1}:${CLASSPATH2}:${CLASSPATH}
    ````

## Build `libhdfs`

Building `libhdfs` is sometimes toublesome on some platforms. Try the instructions below if you are having trouble.

### MacOS (tested with Hadoop sources 1.0.4 and 1.2.1)

- Download hadoop sources tarball and uncompress into folder `sources`
- `cd sources/hadoop-1.x.x/src/c++/libhdfs`
- `chmod +x ./configure`
- `./configure`
- This should create a new file `Makefile` in the current directory.
- Open `Makefile` with a text editor.
    - Look for a line starting with `CFLAGS = -g -O2`. From this line, remove parameter `-m` and add parameter `-framework JavaVM`.
    - Search for a line starting with `DEFS = -DPACKAGE_NAME=\"libhdfs\"`. If it has a parameter `-Dsize_t=unsigned\ int`, remove it.
    - Save and exit
- Open source `hdfsJniHelper.c`
    - Comment out the line starting with `#include <error.h>`
    - Save and exit
- `make`
- `chmod +x ./install-sh`
- `make install`
- If everything went fine till now you can find the built binaries at `sources/hadoop-1.x.x/src/c++/install/lib`.
- Copy these libraries either to `/usr/local/lib` or to someplace added to `DYLD_LIBRARY_PATH` environment variable.


