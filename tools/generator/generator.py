#!/usr/bin/python
import sys

arg_type_set = set()

class Arg:
    def __init__(self, arg_str):
        # e.g., const char* str
        # e.g., hid_t fild_id
        arg_str = arg_str.strip()
        space_idx = arg_str.rfind(" ")
        self.arg_type = arg_str[0:space_idx]
        self.arg_name = arg_str[space_idx+1:]

        clean_arg_type = self.arg_type.replace("const", "")
        clean_arg_type = clean_arg_type.strip()
        self.clean_arg_type = clean_arg_type

        clean_arg_name = self.arg_name
        if self.arg_name.find("[") != -1:
            clean_arg_name = self.arg_name[0:self.arg_name.find("[")]
        clean_arg_name = clean_arg_name.replace("*", "")
        self.clean_arg_name = clean_arg_name

        arg_type_set.add(self.clean_arg_type)

    def __str__(self):
        return "%s %s" %(self.arg_type, self.arg_name)

    # ordinary type, i.e., not pointer, not array
    def is_type_ord(self):
        if "*" not in self.arg_type and "*" not in self.arg_name:
            if "[" not in self.arg_type and "[" not in self.arg_name:
                return True
        return False

    def is_type_ptr(self):
        star_count = 0
        star_count += self.arg_type.count("*")
        star_count += self.arg_name.count("*")
        if star_count == 1:
            if "[]" not in self.arg_name and "[]" not in self.arg_type:
                return True
        return False

    def is_type_ptrptr(self):
        star_count = 0
        star_count += self.arg_type.count("*")
        star_count += self.arg_name.count("*")
        if star_count == 2:
            if "[]" not in self.arg_name and "[]" not in self.arg_type:
                return True
        return False

    # this is to make sure var in int *var
    # is not NULL
    def get_safe_star_ptr(self):
        return "(%s==NULL)?-1:*%s" %(self.clean_arg_name, self.clean_arg_name)

    
    # possible outcome:
    # 1. itoa() for all integers, e.g., size_t, hid_t, H5_index_t
    # 2. ftoa() for all floating point values
    # 3. strtoa() for char*
    # 4. realrealpath() for const char* filename
    # 5. arrtostr()
    # 6. ptoa() for pointers or arguments we don't care
    def get_assemble_arg(self):

        # PnetCDF possible arg types: ['MPI_Offset', 'MPI_Comm', 'unsigned char', 
        # 'char', 'short', 'long', 'unsigned long long', 'long long', 'MPI_Info', 
        # 'unsigned short', 'float', 'signed char', 'MPI_Offset*', 'MPI_Datatype', 
        # 'double', 'int', 'unsigned int', 'void']

        # HDF5 possible arg types: ['H5O_native_info_t', 'H5FD_mem_t', 'unsigned long',
        # 'H5P_cls_create_func_t', 'H5P_prp_get_func_t', 'H5D_mpio_actual_chunk_opt_mode_t',
        # 'H5O_info1_t', 'hbool_t', 'H5L_info1_t', 'H5P_cls_copy_func_t', 'H5LT_lang_t',
        # 'H5L_elink_traverse_t', 'H5ES_err_info_t', 'H5I_future_discard_func_t',
        # 'H5FD_mpio_xfer_t', 'H5E_major_t', 'H5F_file_space_type_t', 'H5T_cset_t', 
        # 'haddr_t', 'H5O_mcdt_search_cb_t', 'H5VL_class_value_t', 'H5A_operator2_t', 
        # 'H5S_sel_type', 'H5F_flush_cb_t', 'H5F_info2_t', 'H5Z_filter_func_t', 
        # 'H5A_operator1_t', 'float', 'H5T_conv_except_func_t', 'H5D_mpio_actual_io_mode_t', 
        # 'H5ES_event_complete_func_t', 'H5I_future_realize_func_t', '..', 'H5O_iterate2_t', 
        # 'H5F_close_degree_t', 'H5O_type_t', 'H5MM_free_t', 'H5FD_mirror_fapl_t', 
        # 'H5_index_t', 'H5T_class_t', 'H5F_sect_info_t', 'H5P_prp_create_func_t', 'int', 
        # 'H5FD_mpio_collective_opt_t', 'char', 'unsigned', 'H5FD_onion_fapl_info_t', 
        # 'H5E_type_t', 'H5D_selection_io_mode_t', 'H5P_prp_compare_func_t', 'H5D_layout_t', 
        # 'H5D_alloc_time_t', 'H5VL_subclass_t', 'unsigned short', 'H5D_operator_t', 'long', 
        # 'H5F_scope_t', 'H5FD_class_value_t', 'H5D_gather_func_t', 'H5O_info2_t', 
        # 'H5P_cls_close_func_t', 'H5L_info2_t', 'H5P_iterate_t', 'void', 'FILE', 
        # 'H5L_iterate2_t', 'H5AC_cache_config_t', 'off_t', 'H5P_prp_close_func_t', 
        # 'H5F_libver_t', 'unsigned char', 'H5E_auto2_t', 'H5D_space_status_t', 
        # 'H5S_seloper_t', 'H5E_direction_t', 'unsigned int', 'H5Z_filter_t', 
        # 'H5D_fill_value_t', 'hsize_t', 'MPI_Info', 'H5_atclose_func_t', 'hid_t', 'long long', 
        # 'H5FD_subfiling_config_t', 'H5L_type_t', 'H5I_type_t', 'H5F_fspace_strategy_t', 
        # 'H5T_direction_t', 'H5FD_ioc_config_t', 'H5F_info1_t', 'H5R_ref_t', 'H5S_class_t', 
        # 'hdset_reg_ref_t', 'H5ES_event_insert_func_t', 'H5FD_ros3_fapl_t', 'H5Z_SO_scale_type_t', 
        # 'H5FD_mpio_chunk_opt_t', 'H5G_iterate_t', 'H5D_chunk_iter_op_t', 'short', 'uint32_t', 
        # 'H5E_walk1_t', 'H5P_prp_delete_func_t', 'unsigned long long', 'size_t', 
        # 'H5MM_allocate_t', 'H5E_auto1_t', 'H5P_prp_copy_func_t', 'H5FD_splitter_vfd_config_t', 
        # 'H5D_fill_time_t', 'H5G_info_t', 'H5R_type_t', 'H5Z_EDC_t', 'H5A_info_t', 'uint64_t', 
        # 'MPI_Comm', 'H5O_iterate1_t', 'H5E_walk2_t', 'H5O_token_t', 'hssize_t', 'H5E_minor_t', 
        # 'double', 'H5AC_cache_image_config_t', 'H5P_prp_set_func_t', 'H5G_stat_t', 'H5D_scatter_func_t', 
        # 'char *', 'H5_iter_order_t', 'H5FD_hdfs_fapl_t', 'H5F_mem_t', 'H5DS_iterate_t', 
        # 'H5FD_file_image_callbacks_t'

        # NetCDF 4.9.2 Arg types:
        # 'NC_memio', 'nc_type', 'void', 'NC_Dispatch', 'unsigned', 'size_t', 'long long', 'char', 
        # 'double', 'signed char', 'nc_vlen_t', 'MPI_Info', 'unsigned short', 'int', 'MPI_Comm', 
        # 'float', 'unsigned int', 'ptrdiff_t', 'long', 'unsigned long long', 'unsigned char', 'short'

        if self.is_type_ord() or self.is_type_ptr():
            # itoa
            if self.clean_arg_type in ["MPI_Offset", "short", "long", "long long", "unsigned long long", \
                    "unsigned short", "int", "unsigned int", "nc_type", \
                    "unsigned long", "uint32_t", "uint64_t", "hssize_t", \
                    "H5_index_t", "H5_iter_order_t", "H5F_scope_t", "hsize_t", "hid_t", "unsigned", "hbool_t", "size_t"]:
                # TODO how about ptrptr type?
                return "itoa(%s)" %(self.get_safe_star_ptr() if self.is_type_ptr() else self.clean_arg_name)
            # ftoa
            elif self.clean_arg_type in ["float", "double"]:
                # TODO how about ptrptr type?
                return "ftoa(%s)" %(self.get_safe_star_ptr() if self.is_type_ptr() else self.clean_arg_name)
            # strtoa
            elif self.clean_arg_type in ["char", "unsigned char", "char *", "signed char"]:
                if self.is_type_ptr():
                    return "strtoa(%s)" %(self.clean_arg_name)
                elif self.is_type_ptrptr():
                    return "strtoa(%s)" %(self.get_safe_star_ptr())
        if self.clean_arg_type in []:
            # TODO: handle HDF5 args like bypasses[2]
            return "arrtostr(%s)" %(self.clean_arg_name)

        # ptoa
        # handle all the rest
        return "ptoa(%s%s)" %("&" if self.is_type_ord() else "", self.clean_arg_name)

        # TODO handle different MPI datatypes; comm2name, type2name, etc.
        # TODO int varids[] is handled wrong: itoa(*varids)


class Func:
    def __init__(self, func_str):
        ret_func_str = func_str.split("(")[0].strip()
        space_idx = ret_func_str.rfind(" ")
        self.ret_type = ret_func_str[0:space_idx]
        self.func_name = ret_func_str[space_idx+1:]
        self.args      = []

        # now parse arguments
        args_str  = func_str.split("(")[1].split(")")[0]
        self.args_str = args_str
        args_str = args_str.replace(", ", ",")
        for arg_str in args_str.split(","):
            if arg_str != "void":
                self.args.append(Arg(arg_str))

    def get_wrapper_signature(self):
        s = "%s WRAPPER_NAME(%s)(%s)" %(self.ret_type, self.func_name, self.args_str)
        return s
    
    def get_arg_names(self):
        arg_names = [arg.clean_arg_name for arg in self.args]
        s = ", ".join(arg_names)
        return s

    def get_assemble_arg_list(self):
        s = []
        for arg in self.args:
            s.append( arg.get_assemble_arg() )
        return ",".join(s)

    def __str__(self):
        return "%s %s args: %d" %(self.ret_type, self.func_name, len(self.args))


def read_funcs(path):
    lines = []
    with open(path) as file:
        lines = [line.rstrip() for line in file]

    funcs = []
    for line in lines:
        func_str = " ".join(line.split())  # Substitute multiple whitespace with single whitespace 
        f = Func(func_str)
        funcs.append(f)

    return funcs


"""
add in recorder-logger.h
"""
def print_func_names(funcs):
    func_name_set = set()
    s = ""
    i = 0
    for f in funcs:
        if f.func_name not in func_name_set:
            func_name_set.add(f.func_name)
        else:
            print("huh???%s" %f.func_name)
        s += ("\"" + f.func_name + "\"")
        i += 1
        if i % 3 == 0:
            s += ",\n"
        else:
            s += ", "
    print(s)


"""
add in lib/recorder-xxx.c (e.g., recorder-hdf5.c)
"""
def print_gotcha_wrappers(funcs):
    for f in funcs:
        s  = f.get_wrapper_signature() + " {\n"
        s += "\tRECORDER_INTERCEPTOR_PROLOGUE(%s, %s, (%s));\n" %(f.ret_type, f.func_name, f.get_arg_names())
        if len(f.args) == 0:
            s += "\tchar **args = NULL;\n"
        else:
            s += "\tchar **args = assemble_args_list(%d, %s);\n"  %(len(f.args), f.get_assemble_arg_list())
        s += "\tRECORDER_INTERCEPTOR_EPILOGUE(%d, args);\n" %(len(f.args))
        s += "}\n"
        print(s)
        
"""
add in lib/recorder-gotcha.c
"""
def print_gotcha_bindings(funcs):
    for f in funcs:
        s = "GOTCHA_WRAP_ACTION(%s)," %(f.func_name)
        print(s)

"""
add in include/recorder-gotcha.h
"""
def print_gotcha_wrappees(funcs):
    for f in funcs:
        s = "GOTCHA_WRAP(%s, %s, (%s));" %(f.func_name, f.ret_type, f.args_str)
        print(s)

if __name__ == "__main__":

    funcs = read_funcs(sys.argv[1])
    #print("Arg type set:")
    #print(arg_type_set)

    # 1. add in include/recorder-logger.h
    #print_func_names(funcs)

    # 2. add in include/recorder-gotcha.h
    #print_gotcha_wrappees(funcs)

    # 3. add in lib/recorder-gotcha.c
    #print_gotcha_bindings(funcs)

    # 4. add in lib/recorder-xxx.c (e.g., recorder-hdf5.c)
    print_gotcha_wrappers(funcs)
