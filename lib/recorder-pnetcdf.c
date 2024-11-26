#define _XOPEN_SOURCE 500
#define _GNU_SOURCE         //  Need to be on top to use RTLD_NEXT

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <limits.h>
#include <errno.h>
#include "mpi.h"
#include "recorder.h"
#include "pnetcdf.h"

const char* WRAPPER_NAME(ncmpi_strerror)(int err) {
	RECORDER_INTERCEPTOR_PROLOGUE(const char*, ncmpi_strerror, (err));
	char **args = assemble_args_list(1, itoa(err));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

const char* WRAPPER_NAME(ncmpi_strerrno)(int err) {
	RECORDER_INTERCEPTOR_PROLOGUE(const char*, ncmpi_strerrno, (err));
	char **args = assemble_args_list(1, itoa(err));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_create)(MPI_Comm comm, const char *path, int cmode, MPI_Info info, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_create, (comm, path, cmode, info, ncidp));
	char **args = assemble_args_list(5, ptoa(&comm),strtoa(path),itoa(cmode),ptoa(&info),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_open)(MPI_Comm comm, const char *path, int omode, MPI_Info info, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_open, (comm, path, omode, info, ncidp));
	char **args = assemble_args_list(5, ptoa(&comm),strtoa(path),itoa(omode),ptoa(&info),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_delete)(const char *filename, MPI_Info info) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_delete, (filename, info));
	char **args = assemble_args_list(2, strtoa(filename),ptoa(&info));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_enddef)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_enddef, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi__enddef)(int ncid, MPI_Offset h_minfree, MPI_Offset v_align, MPI_Offset v_minfree, MPI_Offset r_align) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi__enddef, (ncid, h_minfree, v_align, v_minfree, r_align));
	char **args = assemble_args_list(5, itoa(ncid),itoa(h_minfree),itoa(v_align),itoa(v_minfree),itoa(r_align));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_redef)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_redef, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_set_default_format)(int format, int *old_formatp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_set_default_format, (format, old_formatp));
	char **args = assemble_args_list(2, itoa(format),itoa((old_formatp==NULL)?-1:*old_formatp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_default_format)(int *formatp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_default_format, (formatp));
	char **args = assemble_args_list(1, itoa((formatp==NULL)?-1:*formatp));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_flush)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_flush, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_sync)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_sync, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_sync_numrecs)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_sync_numrecs, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_abort)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_abort, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_begin_indep_data)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_begin_indep_data, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_end_indep_data)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_end_indep_data, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_close)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_close, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_set_fill)(int ncid, int fillmode, int *old_modep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_set_fill, (ncid, fillmode, old_modep));
	char **args = assemble_args_list(3, itoa(ncid),itoa(fillmode),itoa((old_modep==NULL)?-1:*old_modep));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_def_dim)(int ncid, const char *name, MPI_Offset len, int *idp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_def_dim, (ncid, name, len, idp));
	char **args = assemble_args_list(4, itoa(ncid),strtoa(name),itoa(len),itoa((idp==NULL)?-1:*idp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_def_var)(int ncid, const char *name, nc_type xtype, int ndims, const int *dimidsp, int *varidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_def_var, (ncid, name, xtype, ndims, dimidsp, varidp));
	char **args = assemble_args_list(6, itoa(ncid),strtoa(name),itoa(xtype),itoa(ndims),itoa((dimidsp==NULL)?-1:*dimidsp),itoa((varidp==NULL)?-1:*varidp));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_rename_dim)(int ncid, int dimid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_rename_dim, (ncid, dimid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(dimid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_rename_var)(int ncid, int varid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_rename_var, (ncid, varid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

const char* WRAPPER_NAME(ncmpi_inq_libvers)(void) {
	RECORDER_INTERCEPTOR_PROLOGUE(const char*, ncmpi_inq_libvers, ());
	char **args = NULL;
	RECORDER_INTERCEPTOR_EPILOGUE(0, args);
}

int WRAPPER_NAME(ncmpi_inq)(int ncid, int *ndimsp, int *nvarsp, int *ngattsp, int *unlimdimidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq, (ncid, ndimsp, nvarsp, ngattsp, unlimdimidp));
	char **args = assemble_args_list(5, itoa(ncid),itoa((ndimsp==NULL)?-1:*ndimsp),itoa((nvarsp==NULL)?-1:*nvarsp),itoa((ngattsp==NULL)?-1:*ngattsp),itoa((unlimdimidp==NULL)?-1:*unlimdimidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_inq_format)(int ncid, int *formatp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_format, (ncid, formatp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((formatp==NULL)?-1:*formatp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_file_format)(const char *filename, int *formatp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_file_format, (filename, formatp));
	char **args = assemble_args_list(2, strtoa(filename),itoa((formatp==NULL)?-1:*formatp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_version)(int ncid, int *nc_mode) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_version, (ncid, nc_mode));
	char **args = assemble_args_list(2, itoa(ncid),itoa((nc_mode==NULL)?-1:*nc_mode));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_striping)(int ncid, int *striping_size, int *striping_count) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_striping, (ncid, striping_size, striping_count));
	char **args = assemble_args_list(3, itoa(ncid),itoa((striping_size==NULL)?-1:*striping_size),itoa((striping_count==NULL)?-1:*striping_count));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_ndims)(int ncid, int *ndimsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_ndims, (ncid, ndimsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((ndimsp==NULL)?-1:*ndimsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_nvars)(int ncid, int *nvarsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_nvars, (ncid, nvarsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((nvarsp==NULL)?-1:*nvarsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_num_rec_vars)(int ncid, int *num_rec_varsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_num_rec_vars, (ncid, num_rec_varsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((num_rec_varsp==NULL)?-1:*num_rec_varsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_num_fix_vars)(int ncid, int *num_fix_varsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_num_fix_vars, (ncid, num_fix_varsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((num_fix_varsp==NULL)?-1:*num_fix_varsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_natts)(int ncid, int *ngattsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_natts, (ncid, ngattsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((ngattsp==NULL)?-1:*ngattsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_unlimdim)(int ncid, int *unlimdimidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_unlimdim, (ncid, unlimdimidp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((unlimdimidp==NULL)?-1:*unlimdimidp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_dimid)(int ncid, const char *name, int *idp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_dimid, (ncid, name, idp));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(name),itoa((idp==NULL)?-1:*idp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_dim)(int ncid, int dimid, char *name, MPI_Offset *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_dim, (ncid, dimid, name, lenp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(dimid),strtoa(name),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_inq_dimname)(int ncid, int dimid, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_dimname, (ncid, dimid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(dimid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_dimlen)(int ncid, int dimid, MPI_Offset *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_dimlen, (ncid, dimid, lenp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(dimid),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_var)(int ncid, int varid, char *name, nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_var, (ncid, varid, name, xtypep, ndimsp, dimidsp, nattsp));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep),itoa((ndimsp==NULL)?-1:*ndimsp),itoa((dimidsp==NULL)?-1:*dimidsp),itoa((nattsp==NULL)?-1:*nattsp));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_inq_varid)(int ncid, const char *name, int *varidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_varid, (ncid, name, varidp));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(name),itoa((varidp==NULL)?-1:*varidp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_varname)(int ncid, int varid, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_varname, (ncid, varid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_vartype)(int ncid, int varid, nc_type *xtypep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_vartype, (ncid, varid, xtypep));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((xtypep==NULL)?-1:*xtypep));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_varndims)(int ncid, int varid, int *ndimsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_varndims, (ncid, varid, ndimsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ndimsp==NULL)?-1:*ndimsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_vardimid)(int ncid, int varid, int *dimidsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_vardimid, (ncid, varid, dimidsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((dimidsp==NULL)?-1:*dimidsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_varnatts)(int ncid, int varid, int *nattsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_varnatts, (ncid, varid, nattsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((nattsp==NULL)?-1:*nattsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_varoffset)(int ncid, int varid, MPI_Offset *offset) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_varoffset, (ncid, varid, offset));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((offset==NULL)?-1:*offset));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_put_size)(int ncid, MPI_Offset *size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_put_size, (ncid, size));
	char **args = assemble_args_list(2, itoa(ncid),itoa((size==NULL)?-1:*size));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_get_size)(int ncid, MPI_Offset *size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_get_size, (ncid, size));
	char **args = assemble_args_list(2, itoa(ncid),itoa((size==NULL)?-1:*size));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_header_size)(int ncid, MPI_Offset *size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_header_size, (ncid, size));
	char **args = assemble_args_list(2, itoa(ncid),itoa((size==NULL)?-1:*size));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_header_extent)(int ncid, MPI_Offset *extent) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_header_extent, (ncid, extent));
	char **args = assemble_args_list(2, itoa(ncid),itoa((extent==NULL)?-1:*extent));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_file_info)(int ncid, MPI_Info *info_used) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_file_info, (ncid, info_used));
	char **args = assemble_args_list(2, itoa(ncid),ptoa(info_used));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_get_file_info)(int ncid, MPI_Info *info_used) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_file_info, (ncid, info_used));
	char **args = assemble_args_list(2, itoa(ncid),ptoa(info_used));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_malloc_size)(MPI_Offset *size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_malloc_size, (size));
	char **args = assemble_args_list(1, itoa((size==NULL)?-1:*size));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_inq_malloc_max_size)(MPI_Offset *size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_malloc_max_size, (size));
	char **args = assemble_args_list(1, itoa((size==NULL)?-1:*size));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_inq_malloc_list)(void) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_malloc_list, ());
	char **args = NULL;
	RECORDER_INTERCEPTOR_EPILOGUE(0, args);
}

int WRAPPER_NAME(ncmpi_inq_files_opened)(int *num, int *ncids) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_files_opened, (num, ncids));
	char **args = assemble_args_list(2, itoa((num==NULL)?-1:*num),itoa((ncids==NULL)?-1:*ncids));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_recsize)(int ncid, MPI_Offset *recsize) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_recsize, (ncid, recsize));
	char **args = assemble_args_list(2, itoa(ncid),itoa((recsize==NULL)?-1:*recsize));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_def_var_fill)(int ncid, int varid, int no_fill, const void *fill_value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_def_var_fill, (ncid, varid, no_fill, fill_value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(no_fill),ptoa(fill_value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_inq_var_fill)(int ncid, int varid, int *no_fill, void *fill_value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_var_fill, (ncid, varid, no_fill, fill_value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((no_fill==NULL)?-1:*no_fill),ptoa(fill_value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_inq_path)(int ncid, int *pathlen, char *path) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_path, (ncid, pathlen, path));
	char **args = assemble_args_list(3, itoa(ncid),itoa((pathlen==NULL)?-1:*pathlen),strtoa(path));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_inq_att)(int ncid, int varid, const char *name, nc_type *xtypep, MPI_Offset *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_att, (ncid, varid, name, xtypep, lenp));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_inq_attid)(int ncid, int varid, const char *name, int *idp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_attid, (ncid, varid, name, idp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((idp==NULL)?-1:*idp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_inq_atttype)(int ncid, int varid, const char *name, nc_type *xtypep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_atttype, (ncid, varid, name, xtypep));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_inq_attlen)(int ncid, int varid, const char *name, MPI_Offset *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_attlen, (ncid, varid, name, lenp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_inq_attname)(int ncid, int varid, int attnum, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_attname, (ncid, varid, attnum, name));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(attnum),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_copy_att)(int ncid, int varid, const char *name, int ncid_out, int varid_out) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_copy_att, (ncid, varid, name, ncid_out, varid_out));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),strtoa(name),itoa(ncid_out),itoa(varid_out));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_rename_att)(int ncid, int varid, const char *name, const char *newname) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_rename_att, (ncid, varid, name, newname));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(newname));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_del_att)(int ncid, int varid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_del_att, (ncid, varid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_att)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const void *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_text)(int ncid, int varid, const char *name, MPI_Offset len, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_text, (ncid, varid, name, len, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),strtoa(name),itoa(len),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_att_schar)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_schar, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_short)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_short, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_int)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_int, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_float)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_float, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_double)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_double, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_longlong)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_longlong, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_att)(int ncid, int varid, const char *name, void *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_text)(int ncid, int varid, const char *name, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_text, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_schar)(int ncid, int varid, const char *name, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_schar, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_short)(int ncid, int varid, const char *name, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_short, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_int)(int ncid, int varid, const char *name, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_int, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_float)(int ncid, int varid, const char *name, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_float, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_double)(int ncid, int varid, const char *name, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_double, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_longlong)(int ncid, int varid, const char *name, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_longlong, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_att_uchar)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_uchar, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_ubyte)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_ubyte, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_ushort)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_ushort, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_uint)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_uint, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_long)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_long, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_att_ulonglong)(int ncid, int varid, const char *name, nc_type xtype, MPI_Offset len, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_att_ulonglong, (ncid, varid, name, xtype, len, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_att_uchar)(int ncid, int varid, const char *name, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_uchar, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_ubyte)(int ncid, int varid, const char *name, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_ubyte, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_ushort)(int ncid, int varid, const char *name, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_ushort, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_uint)(int ncid, int varid, const char *name, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_uint, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_long)(int ncid, int varid, const char *name, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_long, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_att_ulonglong)(int ncid, int varid, const char *name, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_att_ulonglong, (ncid, varid, name, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_fill_var_rec)(int ncid, int varid, MPI_Offset recno) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_fill_var_rec, (ncid, varid, recno));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa(recno));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var1)(int ncid, int varid, const MPI_Offset index[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1, (ncid, varid, index, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_var1_all)(int ncid, int varid, const MPI_Offset index[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_all, (ncid, varid, index, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_var1_text)(int ncid, int varid, const MPI_Offset index[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_text, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_text_all)(int ncid, int varid, const MPI_Offset index[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_text_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_schar)(int ncid, int varid, const MPI_Offset index[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_schar, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_schar_all)(int ncid, int varid, const MPI_Offset index[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_schar_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_short)(int ncid, int varid, const MPI_Offset index[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_short, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_short_all)(int ncid, int varid, const MPI_Offset index[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_short_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_int)(int ncid, int varid, const MPI_Offset index[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_int, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_int_all)(int ncid, int varid, const MPI_Offset index[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_int_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_float)(int ncid, int varid, const MPI_Offset index[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_float, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_float_all)(int ncid, int varid, const MPI_Offset index[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_float_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_double)(int ncid, int varid, const MPI_Offset index[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_double, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_double_all)(int ncid, int varid, const MPI_Offset index[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_double_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_longlong)(int ncid, int varid, const MPI_Offset index[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_longlong, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_longlong_all)(int ncid, int varid, const MPI_Offset index[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_longlong_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1)(int ncid, int varid, const MPI_Offset index[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1, (ncid, varid, index, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_var1_all)(int ncid, int varid, const MPI_Offset index[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_all, (ncid, varid, index, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_var1_text)(int ncid, int varid, const MPI_Offset index[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_text, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_text_all)(int ncid, int varid, const MPI_Offset index[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_text_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_schar)(int ncid, int varid, const MPI_Offset index[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_schar, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_schar_all)(int ncid, int varid, const MPI_Offset index[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_schar_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_short)(int ncid, int varid, const MPI_Offset index[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_short, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_short_all)(int ncid, int varid, const MPI_Offset index[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_short_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_int)(int ncid, int varid, const MPI_Offset index[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_int, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_int_all)(int ncid, int varid, const MPI_Offset index[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_int_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_float)(int ncid, int varid, const MPI_Offset index[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_float, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_float_all)(int ncid, int varid, const MPI_Offset index[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_float_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_double)(int ncid, int varid, const MPI_Offset index[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_double, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_double_all)(int ncid, int varid, const MPI_Offset index[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_double_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_longlong)(int ncid, int varid, const MPI_Offset index[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_longlong, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_longlong_all)(int ncid, int varid, const MPI_Offset index[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_longlong_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_uchar)(int ncid, int varid, const MPI_Offset index[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_uchar, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_uchar_all)(int ncid, int varid, const MPI_Offset index[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_uchar_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_ushort)(int ncid, int varid, const MPI_Offset index[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_ushort, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_ushort_all)(int ncid, int varid, const MPI_Offset index[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_ushort_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_uint)(int ncid, int varid, const MPI_Offset index[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_uint, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_uint_all)(int ncid, int varid, const MPI_Offset index[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_uint_all, (ncid, varid, index, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_long)(int ncid, int varid, const MPI_Offset index[], const long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_long, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_long_all)(int ncid, int varid, const MPI_Offset index[], const long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_long_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_ulonglong)(int ncid, int varid, const MPI_Offset index[], const unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_ulonglong, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var1_ulonglong_all)(int ncid, int varid, const MPI_Offset index[], const unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var1_ulonglong_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_uchar)(int ncid, int varid, const MPI_Offset index[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_uchar, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_uchar_all)(int ncid, int varid, const MPI_Offset index[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_uchar_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_ushort)(int ncid, int varid, const MPI_Offset index[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_ushort, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_ushort_all)(int ncid, int varid, const MPI_Offset index[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_ushort_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_uint)(int ncid, int varid, const MPI_Offset index[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_uint, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_uint_all)(int ncid, int varid, const MPI_Offset index[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_uint_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_long)(int ncid, int varid, const MPI_Offset index[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_long, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_long_all)(int ncid, int varid, const MPI_Offset index[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_long_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_ulonglong)(int ncid, int varid, const MPI_Offset index[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_ulonglong, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_get_var1_ulonglong_all)(int ncid, int varid, const MPI_Offset index[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var1_ulonglong_all, (ncid, varid, index, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_put_var)(int ncid, int varid, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var, (ncid, varid, buf, bufcount, buftype));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_var_all)(int ncid, int varid, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_all, (ncid, varid, buf, bufcount, buftype));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_var_text)(int ncid, int varid, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_text, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_text_all)(int ncid, int varid, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_text_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_schar)(int ncid, int varid, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_schar, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_schar_all)(int ncid, int varid, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_schar_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_short)(int ncid, int varid, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_short, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_short_all)(int ncid, int varid, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_short_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_int)(int ncid, int varid, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_int, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_int_all)(int ncid, int varid, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_int_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_float)(int ncid, int varid, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_float, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_float_all)(int ncid, int varid, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_float_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_double)(int ncid, int varid, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_double, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_double_all)(int ncid, int varid, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_double_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_longlong)(int ncid, int varid, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_longlong, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_longlong_all)(int ncid, int varid, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_longlong_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var)(int ncid, int varid, void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var, (ncid, varid, buf, bufcount, buftype));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_var_all)(int ncid, int varid, void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_all, (ncid, varid, buf, bufcount, buftype));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_var_text)(int ncid, int varid, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_text, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_text_all)(int ncid, int varid, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_text_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_schar)(int ncid, int varid, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_schar, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_schar_all)(int ncid, int varid, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_schar_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_short)(int ncid, int varid, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_short, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_short_all)(int ncid, int varid, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_short_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_int)(int ncid, int varid, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_int, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_int_all)(int ncid, int varid, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_int_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_float)(int ncid, int varid, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_float, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_float_all)(int ncid, int varid, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_float_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_double)(int ncid, int varid, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_double, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_double_all)(int ncid, int varid, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_double_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_longlong)(int ncid, int varid, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_longlong, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_longlong_all)(int ncid, int varid, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_longlong_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_uchar)(int ncid, int varid, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_uchar, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_uchar_all)(int ncid, int varid, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_uchar_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_ushort)(int ncid, int varid, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_ushort, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_ushort_all)(int ncid, int varid, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_ushort_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_uint)(int ncid, int varid, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_uint, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_uint_all)(int ncid, int varid, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_uint_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_long)(int ncid, int varid, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_long, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_long_all)(int ncid, int varid, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_long_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_ulonglong)(int ncid, int varid, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_ulonglong, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_var_ulonglong_all)(int ncid, int varid, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_var_ulonglong_all, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_uchar)(int ncid, int varid, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_uchar, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_uchar_all)(int ncid, int varid, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_uchar_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_ushort)(int ncid, int varid, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_ushort, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_ushort_all)(int ncid, int varid, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_ushort_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_uint)(int ncid, int varid, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_uint, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_uint_all)(int ncid, int varid, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_uint_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_long)(int ncid, int varid, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_long, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_long_all)(int ncid, int varid, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_long_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_ulonglong)(int ncid, int varid, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_ulonglong, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_get_var_ulonglong_all)(int ncid, int varid, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_var_ulonglong_all, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(ncmpi_put_vara)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara, (ncid, varid, start, count, buf, bufcount, buftype));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_vara_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_all, (ncid, varid, start, count, buf, bufcount, buftype));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_vara_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_text, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_text_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_text_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_schar, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_schar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_schar_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_short, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_short_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_short_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_int, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_int_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_int_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_float, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_float_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_float_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_double, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_double_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_double_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_longlong, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_longlong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_longlong_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara, (ncid, varid, start, count, buf, bufcount, buftype));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_vara_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_all, (ncid, varid, start, count, buf, bufcount, buftype));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_vara_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_text, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_text_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_text_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_schar, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_schar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_schar_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_short, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_short_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_short_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_int, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_int_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_int_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_float, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_float_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_float_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_double, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_double_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_double_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_longlong, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_longlong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_longlong_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_uchar, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_uchar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_uchar_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_ushort, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_ushort_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_ushort_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_uint, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_uint_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_uint_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_long, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_long_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_long_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_ulonglong, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vara_ulonglong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vara_ulonglong_all, (ncid, varid, start, count, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_uchar, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_uchar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_uchar_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_ushort, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_ushort_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_ushort_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_uint, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_uint_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_uint_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_long, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_long_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_long_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_ulonglong, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_get_vara_ulonglong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vara_ulonglong_all, (ncid, varid, start, count, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_put_vars)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars, (ncid, varid, start, count, stride, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_put_vars_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_all, (ncid, varid, start, count, stride, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_put_vars_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_text, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_text_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_text_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_schar, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_schar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_schar_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_short, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_short_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_short_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_int, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_int_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_int_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_float, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_float_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_float_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_double, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_double_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_double_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_longlong, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_longlong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_longlong_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars, (ncid, varid, start, count, stride, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_get_vars_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_all, (ncid, varid, start, count, stride, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_get_vars_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_schar, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_schar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_schar_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_text, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_text_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_text_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_short, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_short_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_short_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_int, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_int_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_int_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_float, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_float_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_float_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_double, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_double_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_double_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_longlong, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_longlong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_longlong_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_uchar, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_uchar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_uchar_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_ushort, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_ushort_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_ushort_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_uint, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_uint_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_uint_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_long, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_long_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_long_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_ulonglong, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vars_ulonglong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vars_ulonglong_all, (ncid, varid, start, count, stride, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_uchar, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_uchar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_uchar_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_ushort, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_ushort_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_ushort_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_uint, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_uint_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_uint_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_long, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_long_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_long_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_ulonglong, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vars_ulonglong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vars_ulonglong_all, (ncid, varid, start, count, stride, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varm)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_put_varm_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_all, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_put_varm_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_text, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_text_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_text_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_schar, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_schar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_schar_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_short, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_short_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_short_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_int, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_int_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_int_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_float, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_float_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_float_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_double, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_double_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_double_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_longlong, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_longlong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_longlong_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_get_varm_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_all, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_get_varm_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_schar, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_schar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_schar_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_text, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_text_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_text_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_short, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_short_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_short_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_int, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_int_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_int_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_float, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_float_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_float_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_double, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_double_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_double_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_longlong, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_longlong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_longlong_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_uchar, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_uchar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_uchar_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_ushort, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_ushort_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_ushort_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_uint, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_uint_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_uint_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_long, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_long_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_long_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_ulonglong, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_put_varm_ulonglong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varm_ulonglong_all, (ncid, varid, start, count, stride, imap, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_uchar, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_uchar_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_uchar_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_ushort, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_ushort_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_ushort_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_uint, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_uint_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_uint_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_long, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_long_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_long_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_ulonglong, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_varm_ulonglong_all)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varm_ulonglong_all, (ncid, varid, start, count, stride, imap, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_wait)(int ncid, int count, int array_of_requests[], int array_of_statuses[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_wait, (ncid, count, array_of_requests, array_of_statuses));
	char **args = assemble_args_list(4, itoa(ncid),itoa(count),ptoa(array_of_requests),ptoa(array_of_statuses));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_wait_all)(int ncid, int count, int array_of_requests[], int array_of_statuses[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_wait_all, (ncid, count, array_of_requests, array_of_statuses));
	char **args = assemble_args_list(4, itoa(ncid),itoa(count),ptoa(array_of_requests),ptoa(array_of_statuses));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_cancel)(int ncid, int num, int *requests, int *statuses) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_cancel, (ncid, num, requests, statuses));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),itoa((requests==NULL)?-1:*requests),itoa((statuses==NULL)?-1:*statuses));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_buffer_attach)(int ncid, MPI_Offset bufsize) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_buffer_attach, (ncid, bufsize));
	char **args = assemble_args_list(2, itoa(ncid),itoa(bufsize));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_buffer_detach)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_buffer_detach, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(ncmpi_inq_buffer_usage)(int ncid, MPI_Offset *usage) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_buffer_usage, (ncid, usage));
	char **args = assemble_args_list(2, itoa(ncid),itoa((usage==NULL)?-1:*usage));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_buffer_size)(int ncid, MPI_Offset *buf_size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_buffer_size, (ncid, buf_size));
	char **args = assemble_args_list(2, itoa(ncid),itoa((buf_size==NULL)?-1:*buf_size));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_inq_nreqs)(int ncid, int *nreqs) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_inq_nreqs, (ncid, nreqs));
	char **args = assemble_args_list(2, itoa(ncid),itoa((nreqs==NULL)?-1:*nreqs));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(ncmpi_iput_var1)(int ncid, int varid, const MPI_Offset index[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1, (ncid, varid, index, buf, bufcount, buftype, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_text)(int ncid, int varid, const MPI_Offset index[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_text, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_schar)(int ncid, int varid, const MPI_Offset index[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_schar, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_short)(int ncid, int varid, const MPI_Offset index[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_short, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_int)(int ncid, int varid, const MPI_Offset index[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_int, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_float)(int ncid, int varid, const MPI_Offset index[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_float, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_double)(int ncid, int varid, const MPI_Offset index[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_double, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_longlong)(int ncid, int varid, const MPI_Offset index[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_longlong, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1)(int ncid, int varid, const MPI_Offset index[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1, (ncid, varid, index, buf, bufcount, buftype, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_schar)(int ncid, int varid, const MPI_Offset index[], signed char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_schar, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ptoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_text)(int ncid, int varid, const MPI_Offset index[], char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_text, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_short)(int ncid, int varid, const MPI_Offset index[], short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_short, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_int)(int ncid, int varid, const MPI_Offset index[], int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_int, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_float)(int ncid, int varid, const MPI_Offset index[], float *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_float, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_double)(int ncid, int varid, const MPI_Offset index[], double *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_double, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_longlong)(int ncid, int varid, const MPI_Offset index[], long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_longlong, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1)(int ncid, int varid, const MPI_Offset index[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1, (ncid, varid, index, buf, bufcount, buftype, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(index),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_text)(int ncid, int varid, const MPI_Offset index[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_text, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_schar)(int ncid, int varid, const MPI_Offset index[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_schar, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_short)(int ncid, int varid, const MPI_Offset index[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_short, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_int)(int ncid, int varid, const MPI_Offset index[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_int, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_float)(int ncid, int varid, const MPI_Offset index[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_float, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_double)(int ncid, int varid, const MPI_Offset index[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_double, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_longlong)(int ncid, int varid, const MPI_Offset index[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_longlong, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_uchar)(int ncid, int varid, const MPI_Offset index[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_uchar, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_ushort)(int ncid, int varid, const MPI_Offset index[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_ushort, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_uint)(int ncid, int varid, const MPI_Offset index[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_uint, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_long)(int ncid, int varid, const MPI_Offset index[], const long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_long, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var1_ulonglong)(int ncid, int varid, const MPI_Offset index[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var1_ulonglong, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_uchar)(int ncid, int varid, const MPI_Offset index[], unsigned char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_uchar, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_ushort)(int ncid, int varid, const MPI_Offset index[], unsigned short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_ushort, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_uint)(int ncid, int varid, const MPI_Offset index[], unsigned int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_uint, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_long)(int ncid, int varid, const MPI_Offset index[], long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_long, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iget_var1_ulonglong)(int ncid, int varid, const MPI_Offset index[], unsigned long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var1_ulonglong, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_uchar)(int ncid, int varid, const MPI_Offset index[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_uchar, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_ushort)(int ncid, int varid, const MPI_Offset index[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_ushort, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_uint)(int ncid, int varid, const MPI_Offset index[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_uint, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_long)(int ncid, int varid, const MPI_Offset index[], const long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_long, (ncid, varid, index, ip, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_bput_var1_ulonglong)(int ncid, int varid, const MPI_Offset index[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var1_ulonglong, (ncid, varid, index, op, request));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),ptoa(index),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_iput_var)(int ncid, int varid, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var, (ncid, varid, buf, bufcount, buftype, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_var_schar)(int ncid, int varid, const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_schar, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_text)(int ncid, int varid, const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_text, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_short)(int ncid, int varid, const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_short, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_int)(int ncid, int varid, const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_int, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_float)(int ncid, int varid, const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_float, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_double)(int ncid, int varid, const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_double, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_longlong)(int ncid, int varid, const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_longlong, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var)(int ncid, int varid, void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var, (ncid, varid, buf, bufcount, buftype, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_var_schar)(int ncid, int varid, signed char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_schar, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_text)(int ncid, int varid, char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_text, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_short)(int ncid, int varid, short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_short, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_int)(int ncid, int varid, int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_int, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_float)(int ncid, int varid, float *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_float, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_double)(int ncid, int varid, double *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_double, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_longlong)(int ncid, int varid, long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_longlong, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var)(int ncid, int varid, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var, (ncid, varid, buf, bufcount, buftype, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_var_schar)(int ncid, int varid, const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_schar, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_text)(int ncid, int varid, const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_text, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_short)(int ncid, int varid, const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_short, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_int)(int ncid, int varid, const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_int, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_float)(int ncid, int varid, const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_float, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_double)(int ncid, int varid, const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_double, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_longlong)(int ncid, int varid, const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_longlong, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_uchar)(int ncid, int varid, const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_uchar, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_ushort)(int ncid, int varid, const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_ushort, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_uint)(int ncid, int varid, const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_uint, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_long)(int ncid, int varid, const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_long, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_var_ulonglong)(int ncid, int varid, const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_var_ulonglong, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_uchar)(int ncid, int varid, unsigned char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_uchar, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_ushort)(int ncid, int varid, unsigned short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_ushort, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_uint)(int ncid, int varid, unsigned int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_uint, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_long)(int ncid, int varid, long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_long, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iget_var_ulonglong)(int ncid, int varid, unsigned long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_var_ulonglong, (ncid, varid, ip, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_uchar)(int ncid, int varid, const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_uchar, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_ushort)(int ncid, int varid, const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_ushort, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_uint)(int ncid, int varid, const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_uint, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_long)(int ncid, int varid, const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_long, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_bput_var_ulonglong)(int ncid, int varid, const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_var_ulonglong, (ncid, varid, op, request));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_iput_vara)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara, (ncid, varid, start, count, buf, bufcount, buftype, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_schar, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_text, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_short, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_int, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_float, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_double, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_longlong, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara, (ncid, varid, start, count, buf, bufcount, buftype, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], signed char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_schar, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_text, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_short, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_int, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], float *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_float, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], double *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_double, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_longlong, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara, (ncid, varid, start, count, buf, bufcount, buftype, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_schar, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_text, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_short, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_int, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_float, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_double, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_longlong, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_uchar, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_ushort, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_uint, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_long, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vara_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vara_ulonglong, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_uchar, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_ushort, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_uint, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_long, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iget_vara_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], unsigned long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vara_ulonglong, (ncid, varid, start, count, ip, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_uchar, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_ushort, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_uint, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_long, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_bput_vara_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vara_ulonglong, (ncid, varid, start, count, op, request));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_vars)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars, (ncid, varid, start, count, stride, buf, bufcount, buftype, request));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_schar, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_text, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_short, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_int, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_float, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_double, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_longlong, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars, (ncid, varid, start, count, stride, buf, bufcount, buftype, request));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], signed char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_schar, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_text, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_short, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_int, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], float *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_float, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], double *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_double, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_longlong, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars, (ncid, varid, start, count, stride, buf, bufcount, buftype, request));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_schar, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_text, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_short, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_int, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_float, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_double, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_longlong, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_uchar, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_ushort, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_uint, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_long, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_vars_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_vars_ulonglong, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_uchar, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_ushort, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_uint, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_long, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_vars_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], unsigned long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_vars_ulonglong, (ncid, varid, start, count, stride, ip, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_uchar, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_ushort, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_uint, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_long, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_vars_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_vars_ulonglong, (ncid, varid, start, count, stride, op, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varm)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype, request));
	char **args = assemble_args_list(10, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_schar, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_text, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_short, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_int, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_float, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_double, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_longlong, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype, request));
	char **args = assemble_args_list(10, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], signed char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_schar, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_text, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_short, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_int, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], float *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_float, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], double *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_double, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_longlong, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm, (ncid, varid, start, count, stride, imap, buf, bufcount, buftype, request));
	char **args = assemble_args_list(10, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_schar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const signed char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_schar, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ptoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_text)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_text, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_short)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_short, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_int)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_int, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_float)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const float *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_float, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_double)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const double *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_double, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),ftoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_longlong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_longlong, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_uchar, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_ushort, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_uint, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_long, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iput_varm_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varm_ulonglong, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned char *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_uchar, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned short *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_ushort, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned int *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_uint, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_long, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_iget_varm_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], unsigned long long *ip, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varm_ulonglong, (ncid, varid, start, count, stride, imap, ip, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((ip==NULL)?-1:*ip),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_uchar)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned char *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_uchar, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),strtoa(op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_ushort)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned short *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_ushort, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_uint)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned int *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_uint, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_long)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_long, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_bput_varm_ulonglong)(int ncid, int varid, const MPI_Offset start[], const MPI_Offset count[], const MPI_Offset stride[], const MPI_Offset imap[], const unsigned long long *op, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varm_ulonglong, (ncid, varid, start, count, stride, imap, op, request));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),ptoa(start),ptoa(count),ptoa(stride),ptoa(imap),itoa((op==NULL)?-1:*op),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_put_varn)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn, (ncid, varid, num, starts, counts, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_put_varn_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_all, (ncid, varid, num, starts, counts, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_get_varn)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn, (ncid, varid, num, starts, counts, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_get_varn_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_all, (ncid, varid, num, starts, counts, buf, bufcount, buftype));
	char **args = assemble_args_list(8, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_put_varn_text)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_text, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_schar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const signed char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_schar, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_short)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_short, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_int)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_int, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_float)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const float *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_float, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_double)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const double *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_double, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_longlong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_longlong, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_text_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_text_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_schar_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const signed char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_schar_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_short_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_short_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_int_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_int_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_float_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const float *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_float_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_double_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const double *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_double_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_longlong_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_longlong_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_text)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_text, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_schar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_schar, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_short)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_short, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_int)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_int, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_float)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], float *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_float, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_double)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], double *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_double, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_longlong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_longlong, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_text_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_text_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_schar_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_schar_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_short_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_short_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_int_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_int_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_float_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], float *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_float_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_double_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], double *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_double_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_longlong_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_longlong_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_uchar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_uchar, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_ushort)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_ushort, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_uint)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_uint, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_long)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_long, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_ulonglong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_ulonglong, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_uchar_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_uchar_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_ushort_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_ushort_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_uint_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_uint_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_long_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_long_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_varn_ulonglong_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_varn_ulonglong_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_uchar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_uchar, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_ushort)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_ushort, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_uint)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_uint, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_long)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_long, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_ulonglong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_ulonglong, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_uchar_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_uchar_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_ushort_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_ushort_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_uint_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_uint_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_long_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_long_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_varn_ulonglong_all)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *buf) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_varn_ulonglong_all, (ncid, varid, num, starts, counts, buf));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_iput_varn)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn, (ncid, varid, num, starts, counts, buf, bufcount, buftype, request));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_iget_varn)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn, (ncid, varid, num, starts, counts, buf, bufcount, buftype, request));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_text)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_text, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_schar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const signed char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_schar, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_short)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const short *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_short, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_int)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const int *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_int, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_float)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const float *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_float, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_double)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const double *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_double, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_longlong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_longlong, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_text)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_text, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_schar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_schar, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_short)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], short *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_short, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_int)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], int *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_int, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_float)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], float *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_float, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_double)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], double *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_double, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_longlong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], long long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_longlong, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_uchar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_uchar, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_ushort)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned short *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_ushort, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_uint)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned int *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_uint, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_long)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_long, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iput_varn_ulonglong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned long long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iput_varn_ulonglong, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_uchar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_uchar, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_ushort)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_ushort, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_uint)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_uint, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_long)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_long, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_iget_varn_ulonglong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_iget_varn_ulonglong, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const void *buf, MPI_Offset bufcount, MPI_Datatype buftype, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn, (ncid, varid, num, starts, counts, buf, bufcount, buftype, request));
	char **args = assemble_args_list(9, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa(bufcount),ptoa(&buftype),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_text)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_text, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_schar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const signed char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_schar, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ptoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_short)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const short *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_short, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_int)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const int *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_int, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_float)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const float *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_float, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_double)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const double *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_double, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),ftoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_longlong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_longlong, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_uchar)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned char *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_uchar, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),strtoa(buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_ushort)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned short *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_ushort, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_uint)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned int *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_uint, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_long)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_long, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_bput_varn_ulonglong)(int ncid, int varid, int num, MPI_Offset* const starts[], MPI_Offset* const counts[], const unsigned long long *buf, int *request) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_bput_varn_ulonglong, (ncid, varid, num, starts, counts, buf, request));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa(num),ptoa(starts),ptoa(counts),itoa((buf==NULL)?-1:*buf),itoa((request==NULL)?-1:*request));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_get_vard)(int ncid, int varid, MPI_Datatype filetype, void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vard, (ncid, varid, filetype, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(&filetype),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_get_vard_all)(int ncid, int varid, MPI_Datatype filetype, void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_get_vard_all, (ncid, varid, filetype, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(&filetype),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vard)(int ncid, int varid, MPI_Datatype filetype, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vard, (ncid, varid, filetype, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(&filetype),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_put_vard_all)(int ncid, int varid, MPI_Datatype filetype, const void *buf, MPI_Offset bufcount, MPI_Datatype buftype) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_put_vard_all, (ncid, varid, filetype, buf, bufcount, buftype));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),ptoa(&filetype),ptoa(buf),itoa(bufcount),ptoa(&buftype));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_var)(int ncid, int num, int varids[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var, (ncid, num, varids, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_var_all)(int ncid, int num, int varids[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_all, (ncid, num, varids, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_var_text)(int ncid, int num, int varids[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_text, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_schar)(int ncid, int num, int varids[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_schar, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_uchar)(int ncid, int num, int varids[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_uchar, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_short)(int ncid, int num, int varids[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_short, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_ushort)(int ncid, int num, int varids[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_ushort, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_int)(int ncid, int num, int varids[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_int, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_uint)(int ncid, int num, int varids[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_uint, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_long)(int ncid, int num, int varids[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_long, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_float)(int ncid, int num, int varids[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_float, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_double)(int ncid, int num, int varids[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_double, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_longlong)(int ncid, int num, int varids[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_longlong, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_ulonglong)(int ncid, int num, int varids[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_ulonglong, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_text_all)(int ncid, int num, int varids[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_text_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_schar_all)(int ncid, int num, int varids[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_schar_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_uchar_all)(int ncid, int num, int varids[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_uchar_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_short_all)(int ncid, int num, int varids[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_short_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_ushort_all)(int ncid, int num, int varids[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_ushort_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_int_all)(int ncid, int num, int varids[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_int_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_uint_all)(int ncid, int num, int varids[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_uint_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_long_all)(int ncid, int num, int varids[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_long_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_float_all)(int ncid, int num, int varids[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_float_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_double_all)(int ncid, int num, int varids[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_double_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_longlong_all)(int ncid, int num, int varids[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_longlong_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var_ulonglong_all)(int ncid, int num, int varids[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var_ulonglong_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mput_var1)(int ncid, int num, int varids[], MPI_Offset* const starts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1, (ncid, num, varids, starts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_all, (ncid, num, varids, starts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_text, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_schar, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_uchar, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_short, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_ushort, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_int, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_uint, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_long, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_float, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_double, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_longlong, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_ulonglong, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_text_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_schar_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_uchar_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_short_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_ushort_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_int_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_uint_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_long_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_float_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_double_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_longlong_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_var1_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_var1_ulonglong_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mput_vara)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara, (ncid, num, varids, starts, counts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_all, (ncid, num, varids, starts, counts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_text, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_schar, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_uchar, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_short, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_ushort, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_int, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_uint, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_long, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_float, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_double, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_longlong, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_ulonglong, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_text_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_schar_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_uchar_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_short_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_ushort_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_int_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_uint_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_long_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_float_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_double_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_longlong_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vara_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vara_ulonglong_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mput_vars)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars, (ncid, num, varids, starts, counts, strides, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(9, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_all, (ncid, num, varids, starts, counts, strides, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(9, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_text, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_schar, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_uchar, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_short, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_ushort, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_int, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_uint, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_long, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_float, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_double, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_longlong, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_ulonglong, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_text_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_schar_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_uchar_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_short_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_ushort_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_int_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_uint_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_long_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_float_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_double_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_longlong_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_vars_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_vars_ulonglong_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mput_varm)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm, (ncid, num, varids, starts, counts, strides, imaps, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(10, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_all, (ncid, num, varids, starts, counts, strides, imaps, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(10, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_text, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_schar, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_uchar, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_short, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_ushort, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_int, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_uint, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_long, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_float, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_double, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_longlong, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_ulonglong, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_text_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_schar_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_uchar_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_short_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_ushort_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_int_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_uint_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_long_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_float_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_double_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_longlong_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mput_varm_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mput_varm_ulonglong_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_var)(int ncid, int num, int varids[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var, (ncid, num, varids, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_var_all)(int ncid, int num, int varids[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_all, (ncid, num, varids, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_var_text)(int ncid, int num, int varids[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_text, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_schar)(int ncid, int num, int varids[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_schar, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_uchar)(int ncid, int num, int varids[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_uchar, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_short)(int ncid, int num, int varids[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_short, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_ushort)(int ncid, int num, int varids[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_ushort, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_int)(int ncid, int num, int varids[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_int, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_uint)(int ncid, int num, int varids[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_uint, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_long)(int ncid, int num, int varids[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_long, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_float)(int ncid, int num, int varids[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_float, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_double)(int ncid, int num, int varids[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_double, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_longlong)(int ncid, int num, int varids[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_longlong, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_ulonglong)(int ncid, int num, int varids[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_ulonglong, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_text_all)(int ncid, int num, int varids[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_text_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_schar_all)(int ncid, int num, int varids[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_schar_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_uchar_all)(int ncid, int num, int varids[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_uchar_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_short_all)(int ncid, int num, int varids[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_short_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_ushort_all)(int ncid, int num, int varids[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_ushort_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_int_all)(int ncid, int num, int varids[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_int_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_uint_all)(int ncid, int num, int varids[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_uint_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_long_all)(int ncid, int num, int varids[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_long_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_float_all)(int ncid, int num, int varids[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_float_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_double_all)(int ncid, int num, int varids[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_double_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_longlong_all)(int ncid, int num, int varids[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_longlong_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var_ulonglong_all)(int ncid, int num, int varids[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var_ulonglong_all, (ncid, num, varids, bufs));
	char **args = assemble_args_list(4, itoa(ncid),itoa(num),ptoa(varids),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(ncmpi_mget_var1)(int ncid, int num, int varids[], MPI_Offset* const starts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1, (ncid, num, varids, starts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_all, (ncid, num, varids, starts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_text, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_schar, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_uchar, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_short, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_ushort, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_int, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_uint, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_long, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_float, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_double, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_longlong, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_ulonglong, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_text_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_schar_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_uchar_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_short_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_ushort_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_int_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_uint_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_long_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_float_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_double_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_longlong_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_var1_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_var1_ulonglong_all, (ncid, num, varids, starts, bufs));
	char **args = assemble_args_list(5, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(ncmpi_mget_vara)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara, (ncid, num, varids, starts, counts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_all, (ncid, num, varids, starts, counts, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_text, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_schar, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_uchar, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_short, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_ushort, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_int, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_uint, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_long, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_float, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_double, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_longlong, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_ulonglong, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_text_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_schar_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_uchar_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_short_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_ushort_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_int_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_uint_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_long_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_float_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_double_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_longlong_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vara_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vara_ulonglong_all, (ncid, num, varids, starts, counts, bufs));
	char **args = assemble_args_list(6, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(ncmpi_mget_vars)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars, (ncid, num, varids, starts, counts, strides, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(9, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_all, (ncid, num, varids, starts, counts, strides, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(9, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(9, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_text, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_schar, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_uchar, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_short, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_ushort, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_int, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_uint, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_long, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_float, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_double, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_longlong, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_ulonglong, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_text_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_schar_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_uchar_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_short_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_ushort_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_int_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_uint_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_long_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_float_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_double_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_longlong_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_vars_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_vars_ulonglong_all, (ncid, num, varids, starts, counts, strides, bufs));
	char **args = assemble_args_list(7, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(ncmpi_mget_varm)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm, (ncid, num, varids, starts, counts, strides, imaps, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(10, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], void *bufs[], MPI_Offset bufcounts[], MPI_Datatype datatypes[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_all, (ncid, num, varids, starts, counts, strides, imaps, bufs, bufcounts, datatypes));
	char **args = assemble_args_list(10, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs),ptoa(bufcounts),ptoa(datatypes));
	RECORDER_INTERCEPTOR_EPILOGUE(10, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_text)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_text, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_schar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_schar, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_uchar)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_uchar, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_short)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_short, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_ushort)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_ushort, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_int)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_int, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_uint)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_uint, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_long)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_long, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_float)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_float, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_double)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_double, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_longlong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_longlong, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_ulonglong)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_ulonglong, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_text_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_text_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_schar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], signed char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_schar_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_uchar_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned char *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_uchar_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_short_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_short_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_ushort_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned short *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_ushort_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_int_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_int_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_uint_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned int *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_uint_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_long_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_long_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_float_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], float *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_float_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_double_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], double *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_double_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_longlong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_longlong_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(ncmpi_mget_varm_ulonglong_all)(int ncid, int num, int varids[], MPI_Offset* const starts[], MPI_Offset* const counts[], MPI_Offset* const strides[], MPI_Offset* const imaps[], unsigned long long *bufs[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, ncmpi_mget_varm_ulonglong_all, (ncid, num, varids, starts, counts, strides, imaps, bufs));
	char **args = assemble_args_list(8, itoa(ncid),itoa(num),ptoa(varids),ptoa(starts),ptoa(counts),ptoa(strides),ptoa(imaps),ptoa(bufs));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}
