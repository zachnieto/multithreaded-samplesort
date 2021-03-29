#include <stdio.h>
#include <stdlib.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <fcntl.h>
#include <math.h>
#include <pthread.h>

#include "float_vec.h"
#include "barrier.h"
#include "utils.h"

// Compares the inputs as floats.
int cmpfunc (const void * a, const void * b) {

	return (*(const float *) a > *(const float *) b)
			- (*(const float *) a < *(const float *) b); 
}

// Quicksort for the floats struct
void
qsort_floats(floats* xs)
{
	qsort(xs->data, xs->size, sizeof(float), cmpfunc);
}

// Determine if the array contains the given element
int contains(floats* array, float element) {

	for (int i = 0; i < array->size; i++) {
		if (array->data[i] == element)
			return 1;
	}
	return 0;
}

// Samples the input data for P processes
floats*
sample(floats* input, long size, int P)
{

	int sample_size = 3*(P-1);
	floats* sample_array = make_floats(0);

	float element = input->data[rand() % size];	

	for (int i = 0; i < sample_size; i++) {
		if (contains(sample_array, element))
			i--;
		else {
			floats_push(sample_array, element);		
		}

		element = input->data[rand() % size];	
	}

	qsort_floats(sample_array);

	floats* samples = make_floats(0);
	floats_push(samples, 0);

	for (int i = 1; i < sample_size; i+=3) {
		floats_push(samples, sample_array->data[i]);
	}

	floats_push(samples, 1000);
	free_floats(sample_array);
	return samples;
}

// Sorts the data for the current process
void
sort_worker(int pnum, floats* input, long size, int P, floats* samps, long* sizes, barrier* bb, char* filename)
{

    floats* xs = make_floats(0);

	for (int i = 0; i < size; i++) {
		if (input->data[i] >= samps->data[pnum] && input->data[i] < samps->data[pnum + 1]) {
			floats_push(xs, input->data[i]);
		}
	}

	sizes[pnum] = xs->size;
	
	printf("%d: start %.04f, count %ld\n", pnum, samps->data[pnum], xs->size);

    qsort_floats(xs);

	barrier_wait(bb);

	int start = 0;
	for (int i = 0; i <= pnum - 1; i++) {
		start += sizes[i];
	}

	int out_fd = open(filename, O_WRONLY | O_CREAT, 0644);
	lseek(out_fd, start * sizeof(float) + sizeof(long), SEEK_SET);

	write(out_fd, xs->data, sizes[pnum] * sizeof(float));

	close(out_fd);

    free_floats(xs);
}

// Arguments for sort_worker_args
typedef struct sort_worker_args {
	int pnum;
	floats* data;
	long size;
	int P;
	floats* samps;
	long* sizes;
	barrier* bb;
	char* filename;
} sort_worker_args;

// Function called by each thread
void* thread_main(void* ptr) {
	sort_worker_args *args = (sort_worker_args*) ptr;
	sort_worker(args->pnum, args->data, args->size, 
				args->P, args->samps, 
				args->sizes, args->bb, args->filename);
	free(ptr);
	return 0;
}

// Spawns all child processes and sorts
void
run_sort_workers(floats* input, long size, int P, floats* samps, long* sizes, barrier* bb, char* filename)
{
    pthread_t kids[P];

	for (int i = 0; i < P; i++) {

		sort_worker_args* args = malloc(sizeof(sort_worker_args));
		args->pnum = i;	
		args->data = input;
		args->size = size;
		args->P = P;
		args->samps = samps;
		args->sizes = sizes;
		args->bb = bb;
		args->filename = filename;
		
		int rv = pthread_create(&(kids[i]), 0, thread_main, args);
		check_rv(rv);
	}

    for (int i = 0; i < P; i++) {
        int *tmp;
		int rv = pthread_join(kids[i], (void**) &tmp);
		check_rv(rv);
    }
}

// Samples the data, and sorts using P processes
void
sample_sort(floats* data, long size, int P, long* sizes, barrier* bb, char* filename)
{
    floats* samps = sample(data, size, P);
    run_sort_workers(data, size, P, samps, sizes, bb, filename);

    free_floats(samps);
}

// Implementation of Parallel Sample Sort
int
main(int argc, char* argv[])
{
    alarm(120);

    if (argc != 4) {
        printf("Usage:\n");
        printf("\t%s P inputfile outputfile\n", argv[0]);
        return 1;
    }

    const int P = atoi(argv[1]);
    const char* input_fname = argv[2];
	char* output_fname = argv[3];

    seed_rng();

    int rv;
    struct stat st;
    rv = stat(input_fname, &st);
    check_rv(rv);

    const int fsize = st.st_size;
    if (fsize < 8) {
        printf("File too small.\n");
        return 1;
    }

    int fd_in = open(input_fname, O_RDONLY, 0644);
	long count;
	read(fd_in, &count, sizeof(long));

	int fd_out = open(output_fname, O_WRONLY | O_CREAT, 0644);
	ftruncate(fd_out, sizeof(long) + count * sizeof(float));
	write(fd_out, &count, sizeof(long));
	close(fd_out);

	floats* inputs = make_floats(count);

	read(fd_in, inputs->data, count * sizeof(float));
	close(fd_in);

	long* sizes = malloc(P * sizeof(long));

    barrier* bb = make_barrier(P);

    sample_sort(inputs, count, P, sizes, bb, output_fname);

    free_barrier(bb);
	free(sizes);
	free_floats(inputs);

    return 0;
}

