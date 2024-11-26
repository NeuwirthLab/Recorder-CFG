#!/usr/bin/env python
# encoding: utf-8
from ctypes import *
import sys, os, glob, struct


class VerifyIORecord(Structure):
    # The fields must be identical as PyRecord in tools/reader.h
    _fields_ = [
            ("func_id",    c_int),
            ("call_depth", c_ubyte),
            ("arg_count",  c_ubyte),
            ("args",       POINTER(c_char_p)),    # Note in python3, args[i] is 'bytes' type
    ]

    # In Python3, self.args[i] is 'bytes' type
    # For compatable reason, we convert it to str type
    # and will only use self.arg_strs[i] to access the filename
    """
    def args_to_strs(self):
        arg_strs = [''] * self.arg_count
        for i in range(self.arg_count):
            if(type(self.args[i]) == str):
                arg_strs[i] = self.args[i]
            else:
                arg_strs[i] = self.args[i].decode('utf-8')
        return arg_strs
    """


"""
self.funcs: a list of supported funcitons
self.nprocs
self.num_records[rank] 
self.records[Rank]: per-rank list of VerifyIORecord
"""
class RecorderReader:

    def str2char_p(self, s):
        return c_char_p( s.encode('utf-8') )
    
    def __init__(self, logs_dir):
        if "RECORDER_INSTALL_PATH" not in os.environ:
            msg="Error:\n"\
                "    RECORDER_INSTALL_PATH environment variable is not set.\n" \
                "    Please set it to the path where you installed Recorder."
            print(msg)
            exit(1)

        recorder_install_path = os.path.abspath(os.environ["RECORDER_INSTALL_PATH"])
        libreader_path = recorder_install_path + "/lib/libreader.so"

        if not os.path.isfile(libreader_path):
            msg="Error:\n"\
                "    Could not find Recorder reader library\n"\
                "    Please make sure Recorder is installed at %s",\
                recorder_install_path
            print(msg)
            exit(1);

        # Load function list and the number of processes
        self.logs_dir = logs_dir
        self.__read_num_procs(self.logs_dir + "/recorder.mt")
        self.__load_func_list(self.logs_dir + "/recorder.mt")

        # Set up C reader library
        # Read all VerifyIORecord
        self.libreader = cdll.LoadLibrary(libreader_path)
        self.libreader.recorder_read_verifyio_records.restype = POINTER(POINTER(VerifyIORecord))
        num_records = (c_size_t * self.nprocs)()
        self.records = self.libreader.recorder_read_verifyio_records(self.str2char_p(self.logs_dir), num_records)
        self.num_records = [0 for x in range(self.nprocs)]
        for rank in range(self.nprocs):
            self.num_records[rank] = num_records[rank]

    # We dont need the entire RecorderMetadata
    # we only need to read the first integer, which
    # is the number of processes
    def __read_num_procs(self, metadata_file):
        with open(metadata_file, 'rb') as f:
            self.nprocs = struct.unpack('i', f.read(4))[0]

    # read supported list of functions from the metadata file
    # invoked in __init__() only
    def __load_func_list(self, metadata_file):
        with open(metadata_file, 'rb') as f:
            f.seek(1024, 0)   # skip the reserved metadata block (fixed 1024 bytes)
            self.funcs = f.read().splitlines()
            self.funcs = [func.decode('utf-8') for func in self.funcs]



if __name__ == "__main__":

    import resource, psutil
    print(resource.getrusage(resource.RUSAGE_SELF))
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)

    reader = RecorderReader(sys.argv[1])

    print(resource.getrusage(resource.RUSAGE_SELF))
    print('RAM Used (GB):', psutil.virtual_memory()[3]/1000000000)
