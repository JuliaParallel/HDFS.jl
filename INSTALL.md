## Installation

- Use `Pkg.add("HDFS")` to install the HDFS.jl module.
- Ensure the `libhdfs` shared library is built and can be loaded. 
- Ensure a `JVM` is installed and its shared libraries can be loaded by `libhdfs`.
- Ensure `libhdfs` and `JVM` dynamic libraries are loadable. They should either be available in standard library path (e.g. `/usr/lib`, `/usr/local/lib`) or their locations be added to appropriate environment variables (e.g. `DYLD_LIBRARY_PATH`, `LD_LIBRARY_PATH`)
- Add hadoop Java libraries to `CLASSPATH`. 
    - E.g. for Hadoop 1.x.x
        ````
        HADOOP_HOME=[installation folder]/hadoop-1.2.1
        CLASSPATH1=$(JARS=("$HADOOP_HOME"/lib/*.jar); IFS=:; echo "${JARS[*]}")
        CLASSPATH2=$(JARS=("$HADOOP_HOME"/*.jar); IFS=:; echo "${JARS[*]}")
        export CLASSPATH=${CLASSPATH1}:${CLASSPATH2}:${CLASSPATH}
        ````
    - And for Hadoop 2.x.x
        ````
        HADOOP_HOME=[installation folder]/hadoop-2.2.0
        export HADOOP_HOME
        HADOOP_CONF_DIR=${HADOOP_HOME}/etc/hadoop
        export HADOOP_CONF_DIR
        CLASSPATH1=$(JARS=("$HADOOP_HOME"/share/hadoop/common/*.jar); IFS=:; echo "${JARS[*]}")
        CLASSPATH2=$(JARS=("$HADOOP_HOME"/share/hadoop/common/lib/*.jar); IFS=:; echo "${JARS[*]}")
        CLASSPATH3=$(JARS=("$HADOOP_HOME"/share/hadoop/hdfs/*.jar); IFS=:; echo "${JARS[*]}")
        CLASSPATH4=$(JARS=("$HADOOP_HOME"/share/hadoop/hdfs/lib/*.jar); IFS=:; echo "${JARS[*]}")
        CLASSPATH5=$(JARS=("$HADOOP_HOME"/share/hadoop/tools/lib/*.jar); IFS=:; echo "${JARS[*]}")
        export CLASSPATH=${CLASSPATH1}:${CLASSPATH2}:${CLASSPATH3}:${CLASSPATH4}:${CLASSPATH}
        ````

## Build `libhdfs`

Building `libhdfs` is sometimes toublesome on some platforms. Try the instructions below if you are having trouble.

### On MacOS with Hadoop 1.x (tested with Hadoop sources 1.0.4 and 1.2.1)

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
- Copy these libraries to the system wide library folder `/usr/lib` or `/usr/local/lib` or to someplace that is added to `DYLD_LIBRARY_PATH` environment variable.

### On MacOS with Hadoop 2.x (tested with Hadoop sources 2.2.0)

- Download hadoop sources tarball and uncompress into folder `sources`
- `cd sources/hadoop-2.x.x-src`
- Open file `hadoop-hdfs-project/hadoop-hdfs/src/main/native/util/posix_util.c` with a text editor
    - Add `#include <limits.h>` at the beginning of the file.
- Open file `hadoop-common-project/hadoop-common/src/main/native/src/org/apache/hadoop/security/JniBasedUnixGroupsNetgroupMapping.c`
    - Search for a line beginning with `if(setnetgrent(cgroup) == 1)` and comment it out. Also comment out the corresponding end `}` of this `if` statement.
- Run `mvn compile -Pnative` to begin compilation.
- Once compilation goes through successfully, the `libhdfs` libraries can be found at `hadoop-hdfs-project/hadoop-hdfs/target/native/target/usr/local/lib`
- Install these libraries to the system wide library folder `/usr/lib` or `/usr/local/lib` or to someplace that is added to `DYLD_LIBRARY_PATH` environment variable.

