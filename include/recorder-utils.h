#ifndef __RECORDER_UTILS_H_
#define __RECORDER_UTILS_H_
#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <mpi.h>

void utils_init();
void utils_finalize();
void* recorder_malloc(size_t size);
void recorder_free(void* ptr, size_t size);
pthread_t recorder_gettid(void);
long get_file_size(const char *filename);       // return the size of a file
int accept_filename(const char *filename);      // if include the file in trace
double recorder_wtime(void);                    // return the timestamp
char* itoa(off64_t val);                        // convert an integer to string
char* ftoa(double val);                         // convert a float to string
char* ptoa(const void* ptr);                    // convert a pointer to string
char* strtoa(const char* ptr);                  // convert a char* to string (safer strdup)
char* arrtoa(size_t arr[], int count);          // convert an array of size_t to a string
char** assemble_args_list(int arg_count, ...);
const char* get_function_name_by_id(int id);
int get_function_id_by_name(const char* name);
char* realrealpath(const char* path);           // return the absolute path (mapped to id in string)
int mkpath(char* file_path, mode_t mode);       // recursive mkdir()


// recorder send/recv/bcast only handles MPI_BYTE stream
void recorder_send( void *buf, size_t count, int dst, int tag, MPI_Comm comm);
void recorder_recv( void *buf, size_t count, int src, int tag, MPI_Comm comm);
void recorder_bcast(void *buf, size_t count, int root, MPI_Comm comm);
void recorder_barrier(MPI_Comm comm);

int min_in_array(int* arr, size_t len);
double recorder_log2(int val);
int recorder_ceil(double val);
/*
 * compress buf using zlib and then write to the output file
 * the file stream must has been opened with write permission.
 */
void recorder_write_zlib(unsigned char* buf, size_t buf_size, FILE* out_file);
int recorder_debug_level();

#define RECORDER_LOG(level, ...)                  \
    do {                                          \
        int debug_level = recorder_debug_level(); \
        if (level <= debug_level) {               \
            /* we do not intercept fprintf */     \
            fprintf(stderr, __VA_ARGS__);         \
        }                                         \
    } while (0)

#define RECORDER_LOGERR(...)  RECORDER_LOG(1, __VA_ARGS__)
#define RECORDER_LOGINFO(...) RECORDER_LOG(2, __VA_ARGS__)
#define RECORDER_LOGDBG(...)  RECORDER_LOG(3, __VA_ARGS__)

#endif
