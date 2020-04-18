#include "parapipe.h"
#include <limits.h>
#include "gstring.h"
#include "vec.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <omp.h>

typedef vec_t(gstr_t*) gstr_vec_t;

static inline gstr_vec_t *init_gstr_vec(){
    gstr_vec_t *gsv = calloc(1, sizeof(gstr_vec_t));
    return gsv;
}

static inline void clean_gstr_vec (gstr_vec_t *gsv) {
    gstr_t *gs = NULL;
    int i;
    vec_foreach(gsv, gs, i) {
        gfree(gs->s);
        gfree(gs);
    };
    vec_deinit(gsv);
}

static inline void fwrite_gstr_vec (gstr_vec_t *gsv, FILE *fp) {
    gstr_t *gs = NULL;
    int i;
    vec_foreach(gsv, gs, i) {
        int ret = fwrite(gs->s, 1, gs->l, fp);
        if (ret != gs->l) {
            perror("fwrite");
        }
    };
}

static inline void destroy_gstr_vec (gstr_vec_t **gsv) {
    clean_gstr_vec(gsv[0]);
    gfree(gsv[0]);
}

struct job {
    char *cmd;
    int  pid;
    int  fdr;
    int  old_fnctl;
    int  fdw;
    FILE *fpw;
    gstr_vec_t *readbuf;
};

extern char **environ;

void subprocess(char *cmd, int *pid, int *fdr, int *fdw) {
    int writepipe[2] = {-1,-1}, /* parent -> child */
        readpipe [2] = {-1,-1}; /* child -> parent */
    pid_t   childpid;
    /*------------------------------------------------------------------------
     * CREATE THE PAIR OF PIPES
     *
     * Pipes have two ends but just one direction: to get a two-way
     * conversation you need two pipes. It's an error if we cannot make
     * them both, and we define these macros for easy reference.
     */
    writepipe[0] = -1;

    if ( pipe(readpipe) < 0  ||  pipe(writepipe) < 0 )
    {
        /* FATAL: cannot create pipe */
        /* close readpipe[0] & [1] if necessary */
    }
    int PARENT_READ = readpipe[0];
    int CHILD_WRITE = readpipe[1];
    int CHILD_READ = writepipe[0];
    int PARENT_WRITE = writepipe[1];
    *fdr = PARENT_READ;
    *fdw = PARENT_WRITE;

    if ( (childpid = fork()) < 0)
    {
        /* FATAL: cannot fork child */
        perror("fork failed");
    }
    else if ( childpid == 0 )   /* in the child */
    {
        close(PARENT_WRITE);
        close(PARENT_READ);

        dup2(CHILD_READ,  0);  close(CHILD_READ);
        dup2(CHILD_WRITE, 1);  close(CHILD_WRITE);
        
        char *a[] = {"/bin/sh", "-c", cmd, NULL};
        execve(*a, a, environ);
        perror("execve failed");
        exit(127);

        /* do child stuff */
    }
    else                /* in the parent */
    {
        close(CHILD_READ);
        close(CHILD_WRITE);
        *pid = childpid;

        /* do parent stuff */
    }
}


void init_job(struct job *job, char *cmd) {
    job->readbuf = init_gstr_vec();
    job->cmd = cmd;
    subprocess(cmd, &job->pid, &job->fdr, &job->fdw);
    int old_fnctl = fcntl(job->fdr, F_GETFL, 0);
    job->old_fnctl = old_fnctl;
    job->fpw = fdopen(job->fdw, "w");
    int retval = fcntl(job->fdr, F_SETFL, old_fnctl | O_NONBLOCK);
    if ( retval < 0) {
        perror("fnctl error");
    }
    retval = fcntl(job->fdr, F_SETPIPE_SZ, 1024*1024);
    if (retval < 0) {
        perror("failed to set pipe size");
        exit(11);
    }
    retval = fcntl(job->fdw, F_SETPIPE_SZ, 1024*1024);
    if (retval < 0) {
        perror("failed to set pipe size");
        exit(11);
    }
}

void read_job(struct job *job) {
#define BUFREADLEN 1024
    char buf[BUFREADLEN];
    int nread;
    while(1) {
        nread = read(job->fdr, buf, BUFREADLEN);
        if (nread > 0) {
            //write(STDOUT_FILENO, buf, nread);
            char *p = buf + nread;
            while (--p >= buf && *p != '\n'){}
            int remain  = 0;
            if (p >= buf) { // with \n
                _Pragma("omp critical")
                {
                    fwrite_gstr_vec(job->readbuf, stdout);
                    //fprintf(stdout, " BUG:zb=%i:pid=%i ", zerobreak, job->pid);
                    fwrite(buf, 1, p - buf + 1, stdout);
                    fflush(stdout);
                }
                clean_gstr_vec(job->readbuf);
            }
            // number of chars after \n
            remain = buf + nread - p - 1;
            if (remain > 0) {
                gstr_t *gs = calloc(1, sizeof(gstr_t));
                gs->l = remain;
                gs->s = malloc(remain);
                memcpy(gs->s, p+1, remain);
                vec_push(job->readbuf, gs);
            }
        } else break;
    }
}
static void fwrite_job(gstr_t *gs, FILE *fp) {
    size_t nwrite = fwrite(gs->s, 1, gs->l, fp);
    if (nwrite != gs->l) {
        fprintf(stderr, "%ld, %ld\n", nwrite, gs->l);
        perror("fwrite");
        exit(11);
    }
}

int parapipe(char *cmd, char *header, int njob, int chunk_nline) {
    struct job jobs[njob];
    memset(jobs, 0, sizeof(struct job));
    for (int i=0; i<njob; i++) {
        struct job *job = &jobs[i];
        init_job(job, cmd);
        if (header != NULL) {
            fprintf(job->fpw, "%s", header);
            fflush(job->fpw);
        }
    }
    gstr_t remain = {0, NULL};
    int partsize = 1024*8;
    int chunk_size = partsize * njob * 2;
    char *chunk = malloc(chunk_size);
    size_t nread = 0;
    while ((nread = fread(chunk, 1, chunk_size, stdin))>0) {
        int npart = (nread - 1) / partsize + 1;
        char *parts[npart];
        _Pragma("omp parallel for") 
            for (int i=0; i<npart; i++) {
                parts[i] = memchr(chunk + i*partsize, '\n', partsize); 
            }

        int npart1 = 0;
        for (int i=0; i<npart; i++) {
            if (parts[i] !=NULL) parts[npart1++] = parts[i];
        }
        npart = npart1;

        _Pragma("omp parallel") {
            int tid = omp_get_thread_num();
            struct job *job = &jobs[tid];
           _Pragma("omp for")
                for (int i=0; i<npart; i++) {
                    read_job(job);
                    if (i==0) {
                        if (remain.s != NULL) fwrite_job(&remain, job->fpw);
                        gstr_t gs; gs.s = chunk; gs.l = parts[i] - chunk + 1;
                        fwrite_job(&gs, job->fpw);
                    } else {
                        gstr_t gs; gs.s = parts[i-1]+1; gs.l = parts[i] - parts[i-1];
                        fwrite_job(&gs, job->fpw);
                    }
                }
        }

        if (npart > 0) {
            gfree(remain.s); remain.l = 0;
            remain.l = chunk+ nread - parts[npart-1] - 1;
            if (remain.l > 0) {
                remain.s = malloc(remain.l);
                memcpy(remain.s, parts[npart-1]+1, remain.l);
            }
        } else {
            gstr_t gs; 
            gs.l = remain.l + nread;
            gs.s = malloc(gs.l);
            if (remain.l > 0) memcpy(gs.s, remain.s, remain.l);
            memcpy(gs.s+remain.l, chunk, nread);
            gfree(remain.s);
            remain = gs;
        }
        for (int i=0; i<njob; i++) { fflush(jobs[i].fpw); }
    }

    if (remain.l > 0) {
        read_job(&jobs[0]);
        fwrite_job(&remain, jobs[0].fpw);
        fflush(jobs[0].fpw);
        gfree(remain.s); remain.l = 0;
    }
    gfree(chunk);
    
    // must close all write-ends, or only the last pipe can be read, I dont know why
    for (int i=0; i<njob; i++) {
        close(jobs[i].fdw);
        fclose(jobs[i].fpw);
    }

    for (int i=0; i<njob; i++) {
        struct job *job = &jobs[i];
        // must change into block mode, ortherwise output incomplete results
        fcntl(job->fdr, F_SETFL, job->old_fnctl);
        read_job(job);
        fwrite_gstr_vec(job->readbuf, stdout);
        fflush(stdout);
        destroy_gstr_vec(&job->readbuf);

        close(job->fdr);
        int status; waitpid(job->pid, &status, 0);
    }
    fflush(stdout);

    return 0;
}
