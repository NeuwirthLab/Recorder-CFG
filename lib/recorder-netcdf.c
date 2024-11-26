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
#include "netcdf.h"

int WRAPPER_NAME(nc__create)(const char *path, int cmode, size_t initialsz, size_t *chunksizehintp, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc__create, (path, cmode, initialsz, chunksizehintp, ncidp));
	char **args = assemble_args_list(5, strtoa(path),itoa(cmode),itoa(initialsz),itoa((chunksizehintp==NULL)?-1:*chunksizehintp),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc__enddef)(int ncid, size_t h_minfree, size_t v_align, size_t v_minfree, size_t r_align) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc__enddef, (ncid, h_minfree, v_align, v_minfree, r_align));
	char **args = assemble_args_list(5, itoa(ncid),itoa(h_minfree),itoa(v_align),itoa(v_minfree),itoa(r_align));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc__open)(const char *path, int omode, size_t *chunksizehintp, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc__open, (path, omode, chunksizehintp, ncidp));
	char **args = assemble_args_list(4, strtoa(path),itoa(omode),itoa((chunksizehintp==NULL)?-1:*chunksizehintp),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_abort)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_abort, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_close)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_close, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_close_memio)(int ncid, NC_memio *memio) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_close_memio, (ncid, memio));
	char **args = assemble_args_list(2, itoa(ncid),ptoa(memio));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_create)(const char *path, int cmode, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_create, (path, cmode, ncidp));
	char **args = assemble_args_list(3, strtoa(path),itoa(cmode),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_create_mem)(const char *path, int mode, size_t initialsize, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_create_mem, (path, mode, initialsize, ncidp));
	char **args = assemble_args_list(4, strtoa(path),itoa(mode),itoa(initialsize),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_create_par)(const char *path, int cmode, MPI_Comm comm, MPI_Info info, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_create_par, (path, cmode, comm, info, ncidp));
	char **args = assemble_args_list(5, strtoa(path),itoa(cmode),ptoa(&comm),ptoa(&info),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_create_par_fortran)(const char *path, int cmode, int comm, int info, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_create_par_fortran, (path, cmode, comm, info, ncidp));
	char **args = assemble_args_list(5, strtoa(path),itoa(cmode),itoa(comm),itoa(info),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_def_user_format)(int mode_flag, NC_Dispatch *dispatch_table, char *magic_number) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_user_format, (mode_flag, dispatch_table, magic_number));
	char **args = assemble_args_list(3, itoa(mode_flag),ptoa(dispatch_table),strtoa(magic_number));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_enddef)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_enddef, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_get_alignment)(int *thresholdp, int *alignmentp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_alignment, (thresholdp, alignmentp));
	char **args = assemble_args_list(2, itoa((thresholdp==NULL)?-1:*thresholdp),itoa((alignmentp==NULL)?-1:*alignmentp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_get_chunk_cache)(size_t *sizep, size_t *nelemsp, float *preemptionp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_chunk_cache, (sizep, nelemsp, preemptionp));
	char **args = assemble_args_list(3, itoa((sizep==NULL)?-1:*sizep),itoa((nelemsp==NULL)?-1:*nelemsp),ftoa((preemptionp==NULL)?-1:*preemptionp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq)(int ncid, int *ndimsp, int *nvarsp, int *nattsp, int *unlimdimidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq, (ncid, ndimsp, nvarsp, nattsp, unlimdimidp));
	char **args = assemble_args_list(5, itoa(ncid),itoa((ndimsp==NULL)?-1:*ndimsp),itoa((nvarsp==NULL)?-1:*nvarsp),itoa((nattsp==NULL)?-1:*nattsp),itoa((unlimdimidp==NULL)?-1:*unlimdimidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_format)(int ncid, int *formatp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_format, (ncid, formatp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((formatp==NULL)?-1:*formatp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_inq_format_extended)(int ncid, int *formatp, int *modep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_format_extended, (ncid, formatp, modep));
	char **args = assemble_args_list(3, itoa(ncid),itoa((formatp==NULL)?-1:*formatp),itoa((modep==NULL)?-1:*modep));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_path)(int ncid, size_t *pathlen, char *path) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_path, (ncid, pathlen, path));
	char **args = assemble_args_list(3, itoa(ncid),itoa((pathlen==NULL)?-1:*pathlen),strtoa(path));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_type)(int ncid, nc_type xtype, char *name, size_t *size) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_type, (ncid, xtype, name, size));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),strtoa(name),itoa((size==NULL)?-1:*size));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_user_format)(int mode_flag, NC_Dispatch **dispatch_table, char *magic_number) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_user_format, (mode_flag, dispatch_table, magic_number));
	char **args = assemble_args_list(3, itoa(mode_flag),ptoa(dispatch_table),strtoa(magic_number));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_open)(const char *path, int omode, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_open, (path, omode, ncidp));
	char **args = assemble_args_list(3, strtoa(path),itoa(omode),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_open_mem)(const char *path, int omode, size_t size, void *memory, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_open_mem, (path, omode, size, memory, ncidp));
	char **args = assemble_args_list(5, strtoa(path),itoa(omode),itoa(size),ptoa(memory),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_open_memio)(const char *path, int omode, NC_memio *params, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_open_memio, (path, omode, params, ncidp));
	char **args = assemble_args_list(4, strtoa(path),itoa(omode),ptoa(params),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_open_par)(const char *path, int omode, MPI_Comm comm, MPI_Info info, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_open_par, (path, omode, comm, info, ncidp));
	char **args = assemble_args_list(5, strtoa(path),itoa(omode),ptoa(&comm),ptoa(&info),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_open_par_fortran)(const char *path, int omode, int comm, int info, int *ncidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_open_par_fortran, (path, omode, comm, info, ncidp));
	char **args = assemble_args_list(5, strtoa(path),itoa(omode),itoa(comm),itoa(info),itoa((ncidp==NULL)?-1:*ncidp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_redef)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_redef, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_set_alignment)(int threshold, int alignment) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_set_alignment, (threshold, alignment));
	char **args = assemble_args_list(2, itoa(threshold),itoa(alignment));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_set_chunk_cache)(size_t size, size_t nelems, float preemption) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_set_chunk_cache, (size, nelems, preemption));
	char **args = assemble_args_list(3, itoa(size),itoa(nelems),ftoa(preemption));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_set_fill)(int ncid, int fillmode, int *old_modep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_set_fill, (ncid, fillmode, old_modep));
	char **args = assemble_args_list(3, itoa(ncid),itoa(fillmode),itoa((old_modep==NULL)?-1:*old_modep));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_sync)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_sync, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_var_par_access)(int ncid, int varid, int par_access) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_var_par_access, (ncid, varid, par_access));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa(par_access));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_def_dim)(int ncid, const char *name, size_t len, int *idp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_dim, (ncid, name, len, idp));
	char **args = assemble_args_list(4, itoa(ncid),strtoa(name),itoa(len),itoa((idp==NULL)?-1:*idp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_dim)(int ncid, int dimid, char *name, size_t *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_dim, (ncid, dimid, name, lenp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(dimid),strtoa(name),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_dimid)(int ncid, const char *name, int *idp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_dimid, (ncid, name, idp));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(name),itoa((idp==NULL)?-1:*idp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_dimlen)(int ncid, int dimid, size_t *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_dimlen, (ncid, dimid, lenp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(dimid),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_dimname)(int ncid, int dimid, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_dimname, (ncid, dimid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(dimid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_ndims)(int ncid, int *ndimsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_ndims, (ncid, ndimsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((ndimsp==NULL)?-1:*ndimsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_inq_unlimdim)(int ncid, int *unlimdimidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_unlimdim, (ncid, unlimdimidp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((unlimdimidp==NULL)?-1:*unlimdimidp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_rename_dim)(int ncid, int dimid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_rename_dim, (ncid, dimid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(dimid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

size_t WRAPPER_NAME(NC_atomictypelen)(nc_type xtype) {
	RECORDER_INTERCEPTOR_PROLOGUE(size_t, NC_atomictypelen, (xtype));
	char **args = assemble_args_list(1, itoa(xtype));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

char * WRAPPER_NAME(NC_atomictypename)(nc_type xtype) {
	RECORDER_INTERCEPTOR_PROLOGUE(char *, NC_atomictypename, (xtype));
	char **args = assemble_args_list(1, itoa(xtype));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_def_var_blosc)(int ncid, int varid, unsigned subcompressor, unsigned level, unsigned blocksize, unsigned addshuffle) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_blosc, (ncid, varid, subcompressor, level, blocksize, addshuffle));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa(subcompressor),itoa(level),itoa(blocksize),itoa(addshuffle));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_inq_var_filter)(int ncid, int varid, unsigned int *idp, size_t *nparamsp, unsigned int *params) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_filter, (ncid, varid, idp, nparamsp, params));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((idp==NULL)?-1:*idp),itoa((nparamsp==NULL)?-1:*nparamsp),itoa((params==NULL)?-1:*params));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_var_filter_ids)(int ncid, int varid, size_t *nfiltersp, unsigned int *ids) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_filter_ids, (ncid, varid, nfiltersp, ids));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((nfiltersp==NULL)?-1:*nfiltersp),itoa((ids==NULL)?-1:*ids));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_var_filter_info)(int ncid, int varid, unsigned int id, size_t *nparamsp, unsigned int *params) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_filter_info, (ncid, varid, id, nparamsp, params));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa(id),itoa((nparamsp==NULL)?-1:*nparamsp),itoa((params==NULL)?-1:*params));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nctypelen)(nc_type type) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nctypelen, (type));
	char **args = assemble_args_list(1, itoa(type));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_def_var)(int ncid, const char *name, nc_type xtype, int ndims, const int *dimidsp, int *varidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var, (ncid, name, xtype, ndims, dimidsp, varidp));
	char **args = assemble_args_list(6, itoa(ncid),strtoa(name),itoa(xtype),itoa(ndims),itoa((dimidsp==NULL)?-1:*dimidsp),itoa((varidp==NULL)?-1:*varidp));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_def_var_fill)(int ncid, int varid, int no_fill, const void *fill_value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_fill, (ncid, varid, no_fill, fill_value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(no_fill),ptoa(fill_value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_def_var_deflate)(int ncid, int varid, int shuffle, int deflate, int deflate_level) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_deflate, (ncid, varid, shuffle, deflate, deflate_level));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa(shuffle),itoa(deflate),itoa(deflate_level));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_def_var_quantize)(int ncid, int varid, int quantize_mode, int nsd) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_quantize, (ncid, varid, quantize_mode, nsd));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(quantize_mode),itoa(nsd));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_def_var_fletcher32)(int ncid, int varid, int fletcher32) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_fletcher32, (ncid, varid, fletcher32));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa(fletcher32));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_def_var_chunking)(int ncid, int varid, int storage, const size_t *chunksizesp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_chunking, (ncid, varid, storage, chunksizesp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(storage),itoa((chunksizesp==NULL)?-1:*chunksizesp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_def_var_endian)(int ncid, int varid, int endian) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_endian, (ncid, varid, endian));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa(endian));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_def_var_szip)(int ncid, int varid, int options_mask, int pixels_per_block) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_var_szip, (ncid, varid, options_mask, pixels_per_block));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(options_mask),itoa(pixels_per_block));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_rename_var)(int ncid, int varid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_rename_var, (ncid, varid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_free_string)(size_t len, char **data) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_free_string, (len, data));
	char **args = assemble_args_list(2, itoa(len),ptoa(data));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_set_var_chunk_cache)(int ncid, int varid, size_t size, size_t nelems, float preemption) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_set_var_chunk_cache, (ncid, varid, size, nelems, preemption));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa(size),itoa(nelems),ftoa(preemption));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_var_chunk_cache)(int ncid, int varid, size_t *sizep, size_t *nelemsp, float *preemptionp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_chunk_cache, (ncid, varid, sizep, nelemsp, preemptionp));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((sizep==NULL)?-1:*sizep),itoa((nelemsp==NULL)?-1:*nelemsp),ftoa((preemptionp==NULL)?-1:*preemptionp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara)(int ncid, int varid, const size_t *startp, const size_t *countp, void *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_text)(int ncid, int varid, const size_t *startp, const size_t *countp, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_text, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_schar)(int ncid, int varid, const size_t *startp, const size_t *countp, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_schar, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_uchar)(int ncid, int varid, const size_t *startp, const size_t *countp, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_uchar, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_short)(int ncid, int varid, const size_t *startp, const size_t *countp, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_short, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_int)(int ncid, int varid, const size_t *startp, const size_t *countp, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_int, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_long)(int ncid, int varid, const size_t *startp, const size_t *countp, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_long, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_float)(int ncid, int varid, const size_t *startp, const size_t *countp, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_float, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_double)(int ncid, int varid, const size_t *startp, const size_t *countp, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_double, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_ubyte)(int ncid, int varid, const size_t *startp, const size_t *countp, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_ubyte, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_ushort)(int ncid, int varid, const size_t *startp, const size_t *countp, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_ushort, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_uint)(int ncid, int varid, const size_t *startp, const size_t *countp, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_uint, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_longlong)(int ncid, int varid, const size_t *startp, const size_t *countp, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_longlong, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_ulonglong)(int ncid, int varid, const size_t *startp, const size_t *countp, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_ulonglong, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_vara_string)(int ncid, int varid, const size_t *startp, const size_t *countp, char **ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vara_string, (ncid, varid, startp, countp, ip));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_get_var1)(int ncid, int varid, const size_t *indexp, void *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_text)(int ncid, int varid, const size_t *indexp, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_text, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_schar)(int ncid, int varid, const size_t *indexp, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_schar, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_uchar)(int ncid, int varid, const size_t *indexp, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_uchar, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_short)(int ncid, int varid, const size_t *indexp, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_short, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_int)(int ncid, int varid, const size_t *indexp, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_int, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_long)(int ncid, int varid, const size_t *indexp, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_long, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_float)(int ncid, int varid, const size_t *indexp, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_float, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_double)(int ncid, int varid, const size_t *indexp, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_double, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_ubyte)(int ncid, int varid, const size_t *indexp, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_ubyte, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_ushort)(int ncid, int varid, const size_t *indexp, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_ushort, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_uint)(int ncid, int varid, const size_t *indexp, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_uint, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_longlong)(int ncid, int varid, const size_t *indexp, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_longlong, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_ulonglong)(int ncid, int varid, const size_t *indexp, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_ulonglong, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var1_string)(int ncid, int varid, const size_t *indexp, char **ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var1_string, (ncid, varid, indexp, ip));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_var)(int ncid, int varid, void *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_text)(int ncid, int varid, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_text, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_schar)(int ncid, int varid, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_schar, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_uchar)(int ncid, int varid, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_uchar, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_short)(int ncid, int varid, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_short, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_int)(int ncid, int varid, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_int, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_long)(int ncid, int varid, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_long, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_float)(int ncid, int varid, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_float, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_double)(int ncid, int varid, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_double, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_ubyte)(int ncid, int varid, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_ubyte, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_ushort)(int ncid, int varid, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_ushort, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_uint)(int ncid, int varid, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_uint, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_longlong)(int ncid, int varid, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_longlong, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_ulonglong)(int ncid, int varid, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_ulonglong, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_var_string)(int ncid, int varid, char **ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_var_string, (ncid, varid, ip));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_vars)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, void *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_text)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_text, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_schar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_schar, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_uchar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_uchar, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_short)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_short, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_int)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_int, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_long)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_long, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_float)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_float, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_double)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_double, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_ubyte)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_ubyte, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_ushort)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_ushort, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_uint)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_uint, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_longlong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_longlong, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_ulonglong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_ulonglong, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_vars_string)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, char **ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_vars_string, (ncid, varid, startp, countp, stridep, ip));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_get_varm)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, void *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_schar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, signed char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_schar, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_uchar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_uchar, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_short)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_short, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_int)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_int, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_long)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_long, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_float)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, float *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_float, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_double)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, double *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_double, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ftoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_ubyte)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, unsigned char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_ubyte, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_ushort)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, unsigned short *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_ushort, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_uint)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, unsigned int *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_uint, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_longlong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_longlong, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_ulonglong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, unsigned long long *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_ulonglong, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((ip==NULL)?-1:*ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_text)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, char *ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_text, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_get_varm_string)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, char **ip) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_varm_string, (ncid, varid, startp, countp, stridep, imapp, ip));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ptoa(ip));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_inq_varid)(int ncid, const char *name, int *varidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_varid, (ncid, name, varidp));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(name),itoa((varidp==NULL)?-1:*varidp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_var)(int ncid, int varid, char *name, nc_type *xtypep, int *ndimsp, int *dimidsp, int *nattsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var, (ncid, varid, name, xtypep, ndimsp, dimidsp, nattsp));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep),itoa((ndimsp==NULL)?-1:*ndimsp),itoa((dimidsp==NULL)?-1:*dimidsp),itoa((nattsp==NULL)?-1:*nattsp));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_inq_varname)(int ncid, int varid, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_varname, (ncid, varid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_vartype)(int ncid, int varid, nc_type *typep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_vartype, (ncid, varid, typep));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((typep==NULL)?-1:*typep));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_varndims)(int ncid, int varid, int *ndimsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_varndims, (ncid, varid, ndimsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((ndimsp==NULL)?-1:*ndimsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_vardimid)(int ncid, int varid, int *dimidsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_vardimid, (ncid, varid, dimidsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((dimidsp==NULL)?-1:*dimidsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_varnatts)(int ncid, int varid, int *nattsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_varnatts, (ncid, varid, nattsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((nattsp==NULL)?-1:*nattsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_var_deflate)(int ncid, int varid, int *shufflep, int *deflatep, int *deflate_levelp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_deflate, (ncid, varid, shufflep, deflatep, deflate_levelp));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((shufflep==NULL)?-1:*shufflep),itoa((deflatep==NULL)?-1:*deflatep),itoa((deflate_levelp==NULL)?-1:*deflate_levelp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_var_fletcher32)(int ncid, int varid, int *fletcher32p) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_fletcher32, (ncid, varid, fletcher32p));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((fletcher32p==NULL)?-1:*fletcher32p));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_var_chunking)(int ncid, int varid, int *storagep, size_t *chunksizesp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_chunking, (ncid, varid, storagep, chunksizesp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((storagep==NULL)?-1:*storagep),itoa((chunksizesp==NULL)?-1:*chunksizesp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_var_fill)(int ncid, int varid, int *no_fill, void *fill_valuep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_fill, (ncid, varid, no_fill, fill_valuep));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((no_fill==NULL)?-1:*no_fill),ptoa(fill_valuep));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_var_quantize)(int ncid, int varid, int *quantize_modep, int *nsdp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_quantize, (ncid, varid, quantize_modep, nsdp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((quantize_modep==NULL)?-1:*quantize_modep),itoa((nsdp==NULL)?-1:*nsdp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_var_endian)(int ncid, int varid, int *endianp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_endian, (ncid, varid, endianp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((endianp==NULL)?-1:*endianp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_var_szip)(int ncid, int varid, int *options_maskp, int *pixels_per_blockp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_var_szip, (ncid, varid, options_maskp, pixels_per_blockp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((options_maskp==NULL)?-1:*options_maskp),itoa((pixels_per_blockp==NULL)?-1:*pixels_per_blockp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_unlimdims)(int ncid, int *nunlimdimsp, int *unlimdimidsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_unlimdims, (ncid, nunlimdimsp, unlimdimidsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa((nunlimdimsp==NULL)?-1:*nunlimdimsp),itoa((unlimdimidsp==NULL)?-1:*unlimdimidsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_vara)(int ncid, int varid, const size_t *startp, const size_t *countp, const void *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_text)(int ncid, int varid, const size_t *startp, const size_t *countp, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_text, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_schar)(int ncid, int varid, const size_t *startp, const size_t *countp, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_schar, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_uchar)(int ncid, int varid, const size_t *startp, const size_t *countp, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_uchar, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_short)(int ncid, int varid, const size_t *startp, const size_t *countp, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_short, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_int)(int ncid, int varid, const size_t *startp, const size_t *countp, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_int, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_long)(int ncid, int varid, const size_t *startp, const size_t *countp, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_long, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_float)(int ncid, int varid, const size_t *startp, const size_t *countp, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_float, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_double)(int ncid, int varid, const size_t *startp, const size_t *countp, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_double, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_ubyte)(int ncid, int varid, const size_t *startp, const size_t *countp, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_ubyte, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_ushort)(int ncid, int varid, const size_t *startp, const size_t *countp, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_ushort, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_uint)(int ncid, int varid, const size_t *startp, const size_t *countp, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_uint, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_longlong)(int ncid, int varid, const size_t *startp, const size_t *countp, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_longlong, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_ulonglong)(int ncid, int varid, const size_t *startp, const size_t *countp, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_ulonglong, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_vara_string)(int ncid, int varid, const size_t *startp, const size_t *countp, const char **op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vara_string, (ncid, varid, startp, countp, op));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_var1)(int ncid, int varid, const size_t *indexp, const void *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_text)(int ncid, int varid, const size_t *indexp, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_text, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_schar)(int ncid, int varid, const size_t *indexp, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_schar, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_uchar)(int ncid, int varid, const size_t *indexp, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_uchar, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_short)(int ncid, int varid, const size_t *indexp, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_short, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_int)(int ncid, int varid, const size_t *indexp, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_int, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_long)(int ncid, int varid, const size_t *indexp, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_long, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_float)(int ncid, int varid, const size_t *indexp, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_float, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_double)(int ncid, int varid, const size_t *indexp, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_double, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_ubyte)(int ncid, int varid, const size_t *indexp, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_ubyte, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_ushort)(int ncid, int varid, const size_t *indexp, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_ushort, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_uint)(int ncid, int varid, const size_t *indexp, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_uint, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_longlong)(int ncid, int varid, const size_t *indexp, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_longlong, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_ulonglong)(int ncid, int varid, const size_t *indexp, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_ulonglong, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var1_string)(int ncid, int varid, const size_t *indexp, const char **op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var1_string, (ncid, varid, indexp, op));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa((indexp==NULL)?-1:*indexp),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_var)(int ncid, int varid, const void *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_text)(int ncid, int varid, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_text, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_schar)(int ncid, int varid, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_schar, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_uchar)(int ncid, int varid, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_uchar, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_short)(int ncid, int varid, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_short, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_int)(int ncid, int varid, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_int, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_long)(int ncid, int varid, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_long, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_float)(int ncid, int varid, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_float, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_double)(int ncid, int varid, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_double, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_ubyte)(int ncid, int varid, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_ubyte, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_ushort)(int ncid, int varid, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_ushort, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_uint)(int ncid, int varid, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_uint, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_longlong)(int ncid, int varid, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_longlong, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_ulonglong)(int ncid, int varid, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_ulonglong, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_var_string)(int ncid, int varid, const char **op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_var_string, (ncid, varid, op));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_put_vars)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const void *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_text)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_text, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_schar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_schar, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_uchar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_uchar, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_short)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_short, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_int)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_int, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_long)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_long, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_float)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_float, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_double)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_double, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_ubyte)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_ubyte, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_ushort)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_ushort, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_uint)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_uint, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_longlong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_longlong, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_ulonglong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_ulonglong, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_vars_string)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const char **op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_vars_string, (ncid, varid, startp, countp, stridep, op));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_varm)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const void *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_text)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_text, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_schar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const signed char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_schar, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_uchar)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_uchar, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_short)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_short, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_int)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_int, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_long)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_long, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_float)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const float *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_float, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_double)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const double *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_double, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ftoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_ubyte)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const unsigned char *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_ubyte, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),strtoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_ushort)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const unsigned short *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_ushort, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_uint)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const unsigned int *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_uint, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_longlong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_longlong, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_ulonglong)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const unsigned long long *op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_ulonglong, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),itoa((op==NULL)?-1:*op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_put_varm_string)(int ncid, int varid, const size_t *startp, const size_t *countp, const ptrdiff_t *stridep, const ptrdiff_t *imapp, const char **op) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_varm_string, (ncid, varid, startp, countp, stridep, imapp, op));
	char **args = assemble_args_list(7, itoa(ncid),itoa(varid),itoa((startp==NULL)?-1:*startp),itoa((countp==NULL)?-1:*countp),ptoa(stridep),ptoa(imapp),ptoa(op));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_inq_att)(int ncid, int varid, const char *name, nc_type *xtypep, size_t *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_att, (ncid, varid, name, xtypep, lenp));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_attid)(int ncid, int varid, const char *name, int *idp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_attid, (ncid, varid, name, idp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((idp==NULL)?-1:*idp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_attname)(int ncid, int varid, int attnum, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_attname, (ncid, varid, attnum, name));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),itoa(attnum),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_natts)(int ncid, int *nattsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_natts, (ncid, nattsp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((nattsp==NULL)?-1:*nattsp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_inq_atttype)(int ncid, int varid, const char *name, nc_type *xtypep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_atttype, (ncid, varid, name, xtypep));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_attlen)(int ncid, int varid, const char *name, size_t *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_attlen, (ncid, varid, name, lenp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_def_grp)(int parent_ncid, const char *name, int *new_ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_grp, (parent_ncid, name, new_ncid));
	char **args = assemble_args_list(3, itoa(parent_ncid),strtoa(name),itoa((new_ncid==NULL)?-1:*new_ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_get_att)(int ncid, int varid, const char *name, void *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ptoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_text)(int ncid, int varid, const char *name, char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_text, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_schar)(int ncid, int varid, const char *name, signed char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_schar, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_uchar)(int ncid, int varid, const char *name, unsigned char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_uchar, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_short)(int ncid, int varid, const char *name, short *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_short, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_int)(int ncid, int varid, const char *name, int *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_int, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_long)(int ncid, int varid, const char *name, long *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_long, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_float)(int ncid, int varid, const char *name, float *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_float, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ftoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_double)(int ncid, int varid, const char *name, double *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_double, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ftoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_ubyte)(int ncid, int varid, const char *name, unsigned char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_ubyte, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_ushort)(int ncid, int varid, const char *name, unsigned short *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_ushort, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_uint)(int ncid, int varid, const char *name, unsigned int *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_uint, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_longlong)(int ncid, int varid, const char *name, long long *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_longlong, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_ulonglong)(int ncid, int varid, const char *name, unsigned long long *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_ulonglong, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_get_att_string)(int ncid, int varid, const char *name, char **value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_get_att_string, (ncid, varid, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),ptoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_put_att_string)(int ncid, int varid, const char *name, size_t len, const char **value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_string, (ncid, varid, name, len, value));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),strtoa(name),itoa(len),ptoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_att_text)(int ncid, int varid, const char *name, size_t len, const char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_text, (ncid, varid, name, len, value));
	char **args = assemble_args_list(5, itoa(ncid),itoa(varid),strtoa(name),itoa(len),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_put_att)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const void *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ptoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_schar)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const signed char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_schar, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_uchar)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const unsigned char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_uchar, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_short)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const short *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_short, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_int)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const int *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_int, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_long)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const long *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_long, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_float)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const float *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_float, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ftoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_double)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const double *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_double, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),ftoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_ubyte)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const unsigned char *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_ubyte, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),strtoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_ushort)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const unsigned short *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_ushort, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_uint)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const unsigned int *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_uint, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_longlong)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const long long *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_longlong, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_put_att_ulonglong)(int ncid, int varid, const char *name, nc_type xtype, size_t len, const unsigned long long *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_put_att_ulonglong, (ncid, varid, name, xtype, len, value));
	char **args = assemble_args_list(6, itoa(ncid),itoa(varid),strtoa(name),itoa(xtype),itoa(len),itoa((value==NULL)?-1:*value));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_rename_att)(int ncid, int varid, const char *name, const char *newname) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_rename_att, (ncid, varid, name, newname));
	char **args = assemble_args_list(4, itoa(ncid),itoa(varid),strtoa(name),strtoa(newname));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_del_att)(int ncid, int varid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_del_att, (ncid, varid, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(varid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_dimids)(int ncid, int *ndims, int *dimids, int include_parents) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_dimids, (ncid, ndims, dimids, include_parents));
	char **args = assemble_args_list(4, itoa(ncid),itoa((ndims==NULL)?-1:*ndims),itoa((dimids==NULL)?-1:*dimids),itoa(include_parents));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_grp_full_ncid)(int ncid, const char *full_name, int *grp_ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grp_full_ncid, (ncid, full_name, grp_ncid));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(full_name),itoa((grp_ncid==NULL)?-1:*grp_ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_grp_ncid)(int ncid, const char *grp_name, int *grp_ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grp_ncid, (ncid, grp_name, grp_ncid));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(grp_name),itoa((grp_ncid==NULL)?-1:*grp_ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_grp_parent)(int ncid, int *parent_ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grp_parent, (ncid, parent_ncid));
	char **args = assemble_args_list(2, itoa(ncid),itoa((parent_ncid==NULL)?-1:*parent_ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_inq_grpname)(int ncid, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grpname, (ncid, name));
	char **args = assemble_args_list(2, itoa(ncid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_inq_grpname_full)(int ncid, size_t *lenp, char *full_name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grpname_full, (ncid, lenp, full_name));
	char **args = assemble_args_list(3, itoa(ncid),itoa((lenp==NULL)?-1:*lenp),strtoa(full_name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_grpname_len)(int ncid, size_t *lenp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grpname_len, (ncid, lenp));
	char **args = assemble_args_list(2, itoa(ncid),itoa((lenp==NULL)?-1:*lenp));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_inq_grps)(int ncid, int *numgrps, int *ncids) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_grps, (ncid, numgrps, ncids));
	char **args = assemble_args_list(3, itoa(ncid),itoa((numgrps==NULL)?-1:*numgrps),itoa((ncids==NULL)?-1:*ncids));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_ncid)(int ncid, const char *name, int *grp_ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_ncid, (ncid, name, grp_ncid));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(name),itoa((grp_ncid==NULL)?-1:*grp_ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_typeids)(int ncid, int *ntypes, int *typeids) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_typeids, (ncid, ntypes, typeids));
	char **args = assemble_args_list(3, itoa(ncid),itoa((ntypes==NULL)?-1:*ntypes),itoa((typeids==NULL)?-1:*typeids));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_varids)(int ncid, int *nvars, int *varids) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_varids, (ncid, nvars, varids));
	char **args = assemble_args_list(3, itoa(ncid),itoa((nvars==NULL)?-1:*nvars),itoa((varids==NULL)?-1:*varids));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_rename_grp)(int grpid, const char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_rename_grp, (grpid, name));
	char **args = assemble_args_list(2, itoa(grpid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_show_metadata)(int ncid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_show_metadata, (ncid));
	char **args = assemble_args_list(1, itoa(ncid));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_inq_type_equal)(int ncid1, nc_type typeid1, int ncid2, nc_type typeid2, int *equal) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_type_equal, (ncid1, typeid1, ncid2, typeid2, equal));
	char **args = assemble_args_list(5, itoa(ncid1),itoa(typeid1),itoa(ncid2),itoa(typeid2),itoa((equal==NULL)?-1:*equal));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_typeid)(int ncid, const char *name, nc_type *typeidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_typeid, (ncid, name, typeidp));
	char **args = assemble_args_list(3, itoa(ncid),strtoa(name),itoa((typeidp==NULL)?-1:*typeidp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_user_type)(int ncid, nc_type xtype, char *name, size_t *size, nc_type *base_nc_typep, size_t *nfieldsp, int *classp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_user_type, (ncid, xtype, name, size, base_nc_typep, nfieldsp, classp));
	char **args = assemble_args_list(7, itoa(ncid),itoa(xtype),strtoa(name),itoa((size==NULL)?-1:*size),itoa((base_nc_typep==NULL)?-1:*base_nc_typep),itoa((nfieldsp==NULL)?-1:*nfieldsp),itoa((classp==NULL)?-1:*classp));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_def_compound)(int ncid, size_t size, const char *name, nc_type *typeidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_compound, (ncid, size, name, typeidp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(size),strtoa(name),itoa((typeidp==NULL)?-1:*typeidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_insert_compound)(int ncid, nc_type xtype, const char *name, size_t offset, nc_type field_typeid) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_insert_compound, (ncid, xtype, name, offset, field_typeid));
	char **args = assemble_args_list(5, itoa(ncid),itoa(xtype),strtoa(name),itoa(offset),itoa(field_typeid));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_insert_array_compound)(int ncid, nc_type xtype, const char *name, size_t offset, nc_type field_typeid, int ndims, const int *dim_sizes) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_insert_array_compound, (ncid, xtype, name, offset, field_typeid, ndims, dim_sizes));
	char **args = assemble_args_list(7, itoa(ncid),itoa(xtype),strtoa(name),itoa(offset),itoa(field_typeid),itoa(ndims),itoa((dim_sizes==NULL)?-1:*dim_sizes));
	RECORDER_INTERCEPTOR_EPILOGUE(7, args);
}

int WRAPPER_NAME(nc_inq_compound)(int ncid, nc_type xtype, char *name, size_t *sizep, size_t *nfieldsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound, (ncid, xtype, name, sizep, nfieldsp));
	char **args = assemble_args_list(5, itoa(ncid),itoa(xtype),strtoa(name),itoa((sizep==NULL)?-1:*sizep),itoa((nfieldsp==NULL)?-1:*nfieldsp));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_compound_name)(int ncid, nc_type xtype, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_name, (ncid, xtype, name));
	char **args = assemble_args_list(3, itoa(ncid),itoa(xtype),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_compound_size)(int ncid, nc_type xtype, size_t *sizep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_size, (ncid, xtype, sizep));
	char **args = assemble_args_list(3, itoa(ncid),itoa(xtype),itoa((sizep==NULL)?-1:*sizep));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_compound_nfields)(int ncid, nc_type xtype, size_t *nfieldsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_nfields, (ncid, xtype, nfieldsp));
	char **args = assemble_args_list(3, itoa(ncid),itoa(xtype),itoa((nfieldsp==NULL)?-1:*nfieldsp));
	RECORDER_INTERCEPTOR_EPILOGUE(3, args);
}

int WRAPPER_NAME(nc_inq_compound_field)(int ncid, nc_type xtype, int fieldid, char *name, size_t *offsetp, nc_type *field_typeidp, int *ndimsp, int *dim_sizesp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_field, (ncid, xtype, fieldid, name, offsetp, field_typeidp, ndimsp, dim_sizesp));
	char **args = assemble_args_list(8, itoa(ncid),itoa(xtype),itoa(fieldid),strtoa(name),itoa((offsetp==NULL)?-1:*offsetp),itoa((field_typeidp==NULL)?-1:*field_typeidp),itoa((ndimsp==NULL)?-1:*ndimsp),itoa((dim_sizesp==NULL)?-1:*dim_sizesp));
	RECORDER_INTERCEPTOR_EPILOGUE(8, args);
}

int WRAPPER_NAME(nc_inq_compound_fieldname)(int ncid, nc_type xtype, int fieldid, char *name) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_fieldname, (ncid, xtype, fieldid, name));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),itoa(fieldid),strtoa(name));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_compound_fieldoffset)(int ncid, nc_type xtype, int fieldid, size_t *offsetp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_fieldoffset, (ncid, xtype, fieldid, offsetp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),itoa(fieldid),itoa((offsetp==NULL)?-1:*offsetp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_compound_fieldtype)(int ncid, nc_type xtype, int fieldid, nc_type *field_typeidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_fieldtype, (ncid, xtype, fieldid, field_typeidp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),itoa(fieldid),itoa((field_typeidp==NULL)?-1:*field_typeidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_compound_fieldndims)(int ncid, nc_type xtype, int fieldid, int *ndimsp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_fieldndims, (ncid, xtype, fieldid, ndimsp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),itoa(fieldid),itoa((ndimsp==NULL)?-1:*ndimsp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_compound_fielddim_sizes)(int ncid, nc_type xtype, int fieldid, int *dim_sizesp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_fielddim_sizes, (ncid, xtype, fieldid, dim_sizesp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),itoa(fieldid),itoa((dim_sizesp==NULL)?-1:*dim_sizesp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_compound_fieldindex)(int ncid, nc_type xtype, const char *name, int *fieldidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_compound_fieldindex, (ncid, xtype, name, fieldidp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),strtoa(name),itoa((fieldidp==NULL)?-1:*fieldidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_def_enum)(int ncid, nc_type base_typeid, const char *name, nc_type *typeidp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_enum, (ncid, base_typeid, name, typeidp));
	char **args = assemble_args_list(4, itoa(ncid),itoa(base_typeid),strtoa(name),itoa((typeidp==NULL)?-1:*typeidp));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_insert_enum)(int ncid, nc_type xtype, const char *name, const void *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_insert_enum, (ncid, xtype, name, value));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),strtoa(name),ptoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_enum)(int ncid, nc_type xtype, char *name, nc_type *base_nc_typep, size_t *base_sizep, size_t *num_membersp) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_enum, (ncid, xtype, name, base_nc_typep, base_sizep, num_membersp));
	char **args = assemble_args_list(6, itoa(ncid),itoa(xtype),strtoa(name),itoa((base_nc_typep==NULL)?-1:*base_nc_typep),itoa((base_sizep==NULL)?-1:*base_sizep),itoa((num_membersp==NULL)?-1:*num_membersp));
	RECORDER_INTERCEPTOR_EPILOGUE(6, args);
}

int WRAPPER_NAME(nc_inq_enum_member)(int ncid, nc_type xtype, int idx, char *name, void *value) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_enum_member, (ncid, xtype, idx, name, value));
	char **args = assemble_args_list(5, itoa(ncid),itoa(xtype),itoa(idx),strtoa(name),ptoa(value));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_inq_enum_ident)(int ncid, nc_type xtype, long long value, char *identifier) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_enum_ident, (ncid, xtype, value, identifier));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),itoa(value),strtoa(identifier));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_free_vlen)(nc_vlen_t *vl) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_free_vlen, (vl));
	char **args = assemble_args_list(1, ptoa(vl));
	RECORDER_INTERCEPTOR_EPILOGUE(1, args);
}

int WRAPPER_NAME(nc_free_vlens)(size_t len, nc_vlen_t vlens[]) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_free_vlens, (len, vlens));
	char **args = assemble_args_list(2, itoa(len),ptoa(vlens));
	RECORDER_INTERCEPTOR_EPILOGUE(2, args);
}

int WRAPPER_NAME(nc_def_vlen)(int ncid, const char *name, nc_type base_typeid, nc_type *xtypep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_vlen, (ncid, name, base_typeid, xtypep));
	char **args = assemble_args_list(4, itoa(ncid),strtoa(name),itoa(base_typeid),itoa((xtypep==NULL)?-1:*xtypep));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_vlen)(int ncid, nc_type xtype, char *name, size_t *datum_sizep, nc_type *base_nc_typep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_vlen, (ncid, xtype, name, datum_sizep, base_nc_typep));
	char **args = assemble_args_list(5, itoa(ncid),itoa(xtype),strtoa(name),itoa((datum_sizep==NULL)?-1:*datum_sizep),itoa((base_nc_typep==NULL)?-1:*base_nc_typep));
	RECORDER_INTERCEPTOR_EPILOGUE(5, args);
}

int WRAPPER_NAME(nc_def_opaque)(int ncid, size_t size, const char *name, nc_type *xtypep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_def_opaque, (ncid, size, name, xtypep));
	char **args = assemble_args_list(4, itoa(ncid),itoa(size),strtoa(name),itoa((xtypep==NULL)?-1:*xtypep));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

int WRAPPER_NAME(nc_inq_opaque)(int ncid, nc_type xtype, char *name, size_t *sizep) {
	RECORDER_INTERCEPTOR_PROLOGUE(int, nc_inq_opaque, (ncid, xtype, name, sizep));
	char **args = assemble_args_list(4, itoa(ncid),itoa(xtype),strtoa(name),itoa((sizep==NULL)?-1:*sizep));
	RECORDER_INTERCEPTOR_EPILOGUE(4, args);
}

