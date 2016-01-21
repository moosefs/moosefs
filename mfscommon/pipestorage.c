
#include "portable.h"

#include <unistd.h>
#include <pthread.h>

#define PS_SIZE 512

static int pipe_inputs[PS_SIZE];
static int pipe_outputs[PS_SIZE];
static uint32_t pipe_first_free = 0;

static pthread_mutex_t glock = PTHREAD_MUTEX_INITIALIZER;

int ps_get_pipe(int pipefd[2]) {
	int res;
	pthread_mutex_lock(&glock);
	if (pipe_first_free==0) {
		res = pipe(pipefd);
	} else {
		pipe_first_free--;
		pipefd[0] = pipe_inputs[pipe_first_free];
		pipefd[1] = pipe_outputs[pipe_first_free];
		res = 0;
	}
	pthread_mutex_unlock(&glock);
	return res;
}

void ps_close_pipe(int pipefd[2]) {
	pthread_mutex_lock(&glock);
	if (pipe_first_free>=PS_SIZE) {
#ifdef WIN32
		closesocket(pipefd[0]);
		closesocket(pipefd[1]);
#else
		close(pipefd[0]);
		close(pipefd[1]);
#endif
	} else {
		pipe_inputs[pipe_first_free] = pipefd[0];
		pipe_outputs[pipe_first_free] = pipefd[1];
		pipe_first_free++;
	}
	pthread_mutex_unlock(&glock);
}
