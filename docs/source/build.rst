Build Recorder
-----------------

There are two ways to build and install Recorder. The first way is to
build Recorder from source using CMake. This is recomended as Recorder
is currently under active development.

1. Building Recorder with CMake (recommended)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Dependencies**

-  MPI
-  HDF5
-  PnetCDF (optional) - Needed for PnetCDF tracing
-  Arrow (optional) > 5.0.0 - Needed for building Parquet convertor.
-  CUDA (optional) - Needed for CUDA kernels interception.


*Note that Recorder and the applications you intend to trace must be
compiled with the same version of HDF5 and MPI.*

**Build & Install**

.. code:: bash

   git clone https://github.com/uiuc-hpc/Recorder.git
   git submodule update --init --recursive
   cd Recorder
   mkdir build
   cd build
   cmake .. -DCMAKE_INSTALL_PREFIX=[install location]
   make
   make install

**CMake Options**

(1) Dependencies install location

If MPI, HDF5, or arrow is not installed in standard locations, you may
need to use ``-DCMAKE_PREFIX_PATH`` to indicate their locations:

.. code:: bash

   cmake ..                                                          \
         -DCMAKE_INSTALL_PREFIX=[install location]                   \
         -DCMAKE_PREFIX_PATH=[semicolon separated depedencies dir]

Recorder can also be built with PnetCDF to trace PnetCDF calls, use `-DRECORDER_WITH_PNETCDF=/path/to/pnetcdf/install`
to build Recorder with the specified PnetCDF.

(2) Intercepting ``fcntl()`` call:

Since v2.1.7, ``fcntl(int fd, int cmd, ...)`` is intercepted. The
commands (2nd argument) defined in POSIX standard are supported. If
non-POSIX commands were used, please disable fcntl tracing at configure
time with ``-DRECORDER_ENABLE_FCNTL_TRACE=OFF``.

(3) Intercepting CUDA kernels:

add ``-DRECORDER_ENABLE_CUDA_TRACE=ON`` to cmake to allow tracing CUDA
kernels.

(4) Parquet Converter

add ``-DRECORDER_ENABLE_PARQUET=ON`` to cmake to build the Parquet
format converter

2. Building Recorder with Spack
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

*NOTE: please do not use Spack to install Recorder for now. The version
there is outdated, we will update it soon.*

For now, building Recorder with Spack provides less flexibility. We will
add the CMake options for spack as well.

.. code:: bash

   spack install recorder

By default Recorder generates traces from all levels, you can use **~**
to disable a specific level.

E.g., the following command will install Recorder with HDF5 and MPI
tracing disabled.

.. code:: bash

   spack install recorder~hdf5~mpi
