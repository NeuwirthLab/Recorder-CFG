#!/usr/bin/env python
# encoding: utf-8
import sys, struct
import numpy as np
import time

'''
Copied from include/recorder-log-format.h
'''
func_list = [
    # POSIX I/O - 66 functions
    "creat",        "creat64",      "open",         "open64",   "close",
    "write",        "read",         "lseek",        "lseek64",  "pread",
    "pread64",      "pwrite",       "pwrite64",     "readv",    "writev",
    "mmap",         "mmap64",       "fopen",        "fopen64",  "fclose",
    "fwrite",       "fread",        "ftell",        "fseek",    "fsync",
    "fdatasync",    "__xstat",      "__xstat64",    "__lxstat", "__lxstat64",
    "__fxstat",     "__fxstat64",   "getcwd",       "mkdir",    "rmdir",
    "chdir",        "link",         "linkat",       "unlink",   "symlink",
    "symlinkat",    "readlink",     "readlinkat",   "rename",   "chmod",
    "chown",        "lchown",       "utime",        "opendir",  "readdir",
    "closedir",     "rewinddir",    "mknod",        "mknodat",  "fcntl",
    "dup",          "dup2",         "pipe",         "mkfifo",   "umask",
    "fdopen",       "fileno",       "access",       "faccessat","tmpfile",
    "remove",

    # MPI I/O  - 71 functions
    "PMPI_File_close",              "PMPI_File_set_size",       "PMPI_File_iread_at",
    "PMPI_File_iread",              "PMPI_File_iread_shared",   "PMPI_File_iwrite_at",
    "PMPI_File_iwrite",             "PMPI_File_iwrite_shared",  "PMPI_File_open",
    "PMPI_File_read_all_begin",     "PMPI_File_read_all",       "PMPI_File_read_at_all",
    "PMPI_File_read_at_all_begin",  "PMPI_File_read_at",        "PMPI_File_read",
    "PMPI_File_read_ordered_begin", "PMPI_File_read_ordered",   "PMPI_File_read_shared",
    "PMPI_File_set_view",           "PMPI_File_sync",           "PMPI_File_write_all_begin",
    "PMPI_File_write_all",          "PMPI_File_write_at_all_begin", "PMPI_File_write_at_all",
    "PMPI_File_write_at",           "PMPI_File_write",          "PMPI_File_write_ordered_begin",
    "PMPI_File_write_ordered",      "PMPI_File_write_shared",   "PMPI_Finalize",
    "PMPI_Finalized",               "PMPI_Init",                "PMPI_Init_thread",
    "PMPI_Wtime",                   "PMPI_Comm_rank",           "PMPI_Comm_size",
    "PMPI_Get_processor_name",      "PMPI_Get_processor_name",  "PMPI_Comm_set_errhandler",
    "PMPI_Barrier",                 "PMPI_Bcast",               "PMPI_Gather",
    "PMPI_Gatherv",                 "PMPI_Scatter",             "PMPI_Scatterv",
    "PMPI_Allgather",               "PMPI_Allgatherv",          "PMPI_Alltoall",
    "PMPI_Reduce",                  "PMPI_Allreduce",           "PMPI_Reduce_scatter",
    "PMPI_Scan",                    "PMPI_Type_commit",         "PMPI_Type_contiguous",
    "PMPI_Type_extent",             "PMPI_Type_free",           "PMPI_Type_hindexed",
    "PMPI_Op_create",               "PMPI_Op_free",             "PMPI_Type_get_envelope",
    "PMPI_Type_size",
    # Added 2019/01/07
    "PMPI_Cart_rank",               "PMPI_Cart_create",         "PMPI_Cart_get",
    "PMPI_Cart_shift",              "PMPI_Wait",                "PMPI_Send",
    "PMPI_Recv",                    "PMPI_Sendrecv",            "PMPI_Isend",
    "PMPI_Irecv",

    # HDF5 I/O - 68 functions
    "H5Fcreate",            "H5Fopen",              "H5Fclose",    "H5Fflush",   # File interface
    "H5Gclose",             "H5Gcreate1",           "H5Gcreate2",   # Group interface
    "H5Gget_objinfo",       "H5Giterate",           "H5Gopen1",
    "H5Gopen2",             "H5Dclose",             "H5Dcreate1",
    "H5Dcreate2",           "H5Dget_create_plist",  "H5Dget_space", # Dataset interface
    "H5Dget_type",          "H5Dopen1",             "H5Dopen2",
    "H5Dread",              "H5Dwrite",             "H5Dset_extent",
    "H5Sclose",
    "H5Screate",            "H5Screate_simple",     "H5Sget_select_npoints",    # Dataspace interface
    "H5Sget_simple_extent_dims", "H5Sget_simple_extent_npoints", "H5Sselect_elements",
    "H5Sselect_hyperslab",  "H5Sselect_none",       "H5Tclose",     # Datatype interface
    "H5Tcopy",              "H5Tget_class",         "H5Tget_size",
    "H5Tset_size",          "H5Tcreate",            "H5Tinsert",
    "H5Aclose",             "H5Acreate1",           "H5Acreate2",   # Attribute interface
    "H5Aget_name",          "H5Aget_num_attrs",     "H5Aget_space",
    "H5Aget_type",          "H5Aopen",              "H5Aopen_idx",
    "H5Aopen_name",         "H5Aread",              "H5Awrite",
    "H5Pclose",             "H5Pcreate",            "H5Pget_chunk", # Property List interface
    "H5Pget_mdc_config",    "H5Pset_alignment",     "H5Pset_chunk",
    "H5Pset_dxpl_mpio",     "H5Pset_fapl_core",     "H5Pset_fapl_mpio",
    "H5Pset_fapl_mpiposix", "H5Pset_istore_k",      "H5Pset_mdc_config",
    "H5Pset_meta_block_size","H5Lexists",           "H5Lget_val",   # Link interface
    "H5Literate",           "H5Oclose",             "H5Oget_info",  # Object interface
    "H5Oget_info_by_name",  "H5Oopen"
]

'''
Global Metadata Structure - same as in include/recorder-log-format.h
    Time Resolution:        double, 8 bytes
    Number of MPI Ranks:    Int, 4 bytes
    Compression Mode:       Int, 4 bytes
    Recorder Window Size:   Int ,4 bytes

    Then followed by one function name per line
    They are the one intercepted by the current version of Recorder
'''
class GlobalMetadata:
    def __init__(self, path):
        self.timeResolution = 0
        self.numRanks = 0
        self.compMode = 0
        self.windowSize = 0
        self.funcs = []

        self.read(path)
        self.output()

    def read(self, path):
        with open(path, 'rb') as f:
            buf = f.read(8+4+4+4)
            self.timeResolution, self.numRanks, \
            self.compMode, self.windowSize = struct.unpack("diii", buf)

            f.seek(24, 0)
            self.funcs = f.read().splitlines()
            self.funcs = [func.replace("PMPI", "MPI") for func in self.funcs]


    def output(self):
        print("Time Resolution:", self.timeResolution)
        print("Ranks:", self.numRanks)
        print("Compression Mode:", self.compMode)
        print("Window Sizes:", self.windowSize)

'''
Local Metadata Structure - same as in include/recorder-log-format.h

    Start timestamp:            double, 8 bytes
    ed timestamp:               double, 8 bytes
    Number of files accessed:   int,    4 bytes
    Total number of records:    int,    4 bytes
    Filemap:                    char**, 8 bytes pointer, ignore it
    File sizes array:           int*,   8 bytes pointer, size of each file accessed, ignore it
    Function counter:           int*256,4 * 256 bytes

    Then one line per file accessed, has the following form:
    file id(4), file size(8), filename length(4), filename(variable length)
'''
class LocalMetadata:
    def __init__(self, path):
        self.tstart = 0
        self.tend = 0
        self.numFiles = 0
        self.totalRecords = 0
        self.functionCounter = []
        self.fileMap = []

        self.read(path)

    def read(self, path):
        with open(path, 'rb') as f:
            self.tstart, self.tend = struct.unpack("dd", f.read(8+8))
            self.numFiles, self.totalRecords = struct.unpack("ii", f.read(4+4))

            # ignore the two pointers
            f.read(8+8)

            self.functionCounter = struct.unpack("i"*256, f.read(256*4))

            self.fileMap = [None] * self.numFiles
            for i in range(self.numFiles):
                fileId = struct.unpack("i", f.read(4))[0]
                fileSize = struct.unpack("l", f.read(8))[0]
                filenameLen = struct.unpack("i", f.read(4))[0]
                filename = f.read(filenameLen)
                self.fileMap[fileId] = [fileId, fileSize, filename]

    def output(self):
        print("Basic Information:")
        print("tstart:", self.tstart)
        print("tend:", self.tend)
        print("files:", self.numFiles)
        print("records:", self.totalRecords)

        print("\nFunction Counter:")
        for idx, count in enumerate(self.functionCounter):
            if count > 0: print(func_list[idx], count)
        print("\nFile Map:")
        for fileInfo in self.fileMap:
            print(fileInfo)

class RecorderReader:
    def __init__(self, path):
        self.globalMetadata = GlobalMetadata(path+"/recorder.mt")
        self.localMetadata = []
        self.records = []

        for rank in range(self.globalMetadata.numRanks):
            self.localMetadata.append( LocalMetadata(path+"/"+str(rank)+".mt") )
            lines = self.read( path+"/"+str(rank)+".itf" )
            records = self.decode(lines)
            records = self.decompress(records)
            # sort records by tstart
            records = sorted(records, key=lambda x: x[1])  # sort by tstart
            self.records.append( records )

    def decompress(self, records):
        for idx, record in enumerate(records):
            if record[0] != 0:
                status, ref_id = record[0], record[3]
                records[idx][3] = records[idx-1-ref_id][3]
                binStr = bin(status & 0b11111111)   # use mask to get the two's complement as in C code
                binStr = binStr[3:][::-1]           # ignore the leading "0b1" and reverse the string

                refArgs = list(records[idx-1-ref_id][4])    # copy the list
                ii = 0
                for i, c in enumerate(binStr):
                    if c == '1':
                        if ii >= len(record[4]):
                            print("Error:", record, ii)
                        refArgs[i] = record[4][ii]
                        ii += 1
                records[idx][4] = refArgs
        return records

    '''
    @lines: The lines read in from one log file
        status:         singed char, 1 byte
        delta_tstart:   int, 4 bytes
        delta_tend:     int, 4 bytes
        funcId/refId:   signed char, 1 byte
        args:           string seperated by space
    Output is a list of records for one rank, where each  record has the format
        [status, tstart, tend, funcId/refId, [args]]
    '''
    def decode(self, lines):
        records = []
        for line in lines:
            status = struct.unpack('b', line[0])[0]
            tstart = struct.unpack('i', line[1:5])[0]
            tend = struct.unpack('i', line[5:9])[0]
            funcId = struct.unpack('B', line[9])[0]
            args = line[11:].split(' ')
            funcname = func_list[funcId]

            # Selective decoding
            #if "H5" not in funcname and "MPI" not in funcname:
            records.append([status, tstart, tend, funcId, args])

        return records


    '''
    Read one rank's trace file and return one line for each record
    '''
    def read(self, path):
        print(path)
        lines = []
        with open(path, 'rb') as f:
            content = f.read()
            start_pos = 0
            end_pos = 0
            while True:
                end_pos = content.find("\n", start_pos+10)
                if end_pos == -1:
                    break
                line = content[start_pos: end_pos]
                lines.append(line)
                start_pos = end_pos+1
        return lines
