#include "parapipe.h"
#include <limits.h>
#include "gstring.h"
#include "vec.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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
    int  out_record_nline;
    FILE *fpw;
    gstr_vec_t *readbuf;
    int  readbuf_nline;
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


void init_job(struct job *job, char *cmd, int out_record_nline) {
    job->out_record_nline = out_record_nline;
    job->readbuf = init_gstr_vec();
    job->readbuf_nline = 0;
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

static const int JOBPARTSIZE = 1024*16;

void read_job(struct job *job) {
    char buf[JOBPARTSIZE];
    int nread;
    while(1) {
        nread = read(job->fdr, buf, JOBPARTSIZE);
        if (nread > 0) {
            char *p = buf - 1;
            if (job->out_record_nline > 1) {
                char *s = buf;
                while(s < buf + nread) {
                    if (*s == '\n' && ++job->readbuf_nline == job->out_record_nline) {
                        p = s;
                        job->readbuf_nline = 0;
                    }
                    s++;
                }
            } else {
                p = buf + nread - 1;
                while(p >= buf && *p != '\n') {
                    p--;
                }
            }
            if (p >= buf) { // with \n
                _Pragma("omp critical")
                {
                    fwrite_gstr_vec(job->readbuf, stdout);
                    fwrite(buf, 1, p - buf + 1, stdout);
                    fflush(stdout);
                }
                clean_gstr_vec(job->readbuf);
            }
            // number of chars after \n
            int remain = buf + nread - p - 1;
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

char *memchr_rev(char *p, int c, int l) {
    char *end = p - l;
    while (p > end) {
        if (*p == c) break;
        p--;
    }
    if (p == end) return NULL;
    else return p;
}

int parapipe(char *cmd, char *header, int njob, gstr_t remain, int record_nline) {
    struct job jobs[njob];
    memset(jobs, 0, sizeof(struct job));
    for (int i=0; i<njob; i++) {
        struct job *job = &jobs[i];
        init_job(job, cmd, record_nline);
        if (header != NULL) {
            fprintf(job->fpw, "%s", header);
            fflush(job->fpw);
        }
    }
    //gstr_t remain = {0, NULL};
    int chunk_size = JOBPARTSIZE * njob * 4;
    char *chunk = malloc(chunk_size);
    size_t nread = 0;
    while ((nread = fread(chunk, 1, chunk_size, stdin))>0) {
        int npart = (nread - 1) / JOBPARTSIZE + 1;
        char *parts[npart];
        _Pragma("omp parallel for") 
            for (int i=0; i<npart; i++) {
                if (i == npart - 1) 
                    parts[i] = memchr_rev(chunk + nread - 1, '\n', nread - i*JOBPARTSIZE); 
                else
                    parts[i] = memchr_rev(chunk + (i+1)*JOBPARTSIZE - 1, '\n', JOBPARTSIZE); 
            }

        int npart1 = 0;
        for (int i=0; i<npart; i++) {
            if (parts[i] !=NULL) parts[npart1++] = parts[i];
        }
        npart = npart1;

        if (record_nline > 1 && npart > 0) {
            int n1 = 0;
            for(char *p = remain.s; p <remain.s + remain.l; p++) {
                if (*p == '\n') n1++; 
            }
            int part_nlines[npart];
            _Pragma("omp parallel for")
                for (int i=0; i<npart; i++) {
                    part_nlines[i] = 0;
                    char *p = parts[i];
                    char *end = i == 0 ? chunk - 1: parts[i-1];
                    while (p > end) {
                        if (*p == '\n') part_nlines[i]++;
                        p--;
                    }
                }

            npart1 = 0;
            for (int i=0; i<npart; i++) {
                n1 += part_nlines[i];
                if (n1 >= record_nline) {
                    n1 = n1 % record_nline;
                    char *p = parts[i] - 1;
                    int  n = n1;
                    while(n > 0) {
                        if (*p == '\n') {
                            n--;
                        }
                        p--;
                    }
                    parts[npart1++] = p + 1;
                }
            }
            npart = npart1;
        }

        _Pragma("omp parallel")
        {
            int tid = omp_get_thread_num();
            struct job *job = &jobs[tid];
                read_job(job);
            _Pragma("omp for schedule(dynamic, 2)")
                for (int i=0; i<npart; i++) {
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

    // must close all write-ends, or only the last pipe can not be read, I dont know why
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
