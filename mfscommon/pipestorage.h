#ifndef _PIPESTORAGE_H_
#define _PIPESTORAGE_H_

int ps_get_pipe(int pipefd[2]);
void ps_close_pipe(int pipefd[2]);

#endif
