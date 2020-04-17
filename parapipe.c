#include "parapipe.h"
#include "gstring.h"
#include "vec.h"
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>

int readlines(gstr_t *ret, int capacity, FILE *fp) {
    char *line = NULL;
    size_t len = 0;
    int nread = -1; 
    int n = 0;;
    for(int i=0; i < capacity; i++) {
        line =  NULL;
        nread = getline(&line, &len, fp);
        if(nread == -1) {
            gfree(line);
            break;
        }
        ret[i] = (gstr_t){nread, line};
        n++;
    }
    return n; 
}

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
        fwrite(gs->s, gs->l, 1, fp);
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
    gstr_vec_t *redbuf;
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
    }
    else if ( childpid == 0 )   /* in the child */
    {
        close(PARENT_WRITE);
        close(PARENT_READ);

        dup2(CHILD_READ,  0);  close(CHILD_READ);
        dup2(CHILD_WRITE, 1);  close(CHILD_WRITE);
        /*
        char buf[111];
        while (1)
        {
            memcpy(buf, "abcdefghi\0",10);
            fwrite(buf, 10, 1, stdout);
            fflush(stdout);
            sleep(1);
        }
        */
        char *a[] = {"/bin/sh", "-c", cmd, NULL};
        execvp(*a, a);
        //execve(*a, a, environ);
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
    job->redbuf = init_gstr_vec();
    job->cmd = cmd;
    subprocess(cmd, &job->pid, &job->fdr, &job->fdw);
    int old_fnctl = fcntl(job->fdr, F_GETFL, 0);
    job->old_fnctl = old_fnctl;
    job->fpw = fdopen(job->fdw, "w");
    int retval = fcntl(job->fdr, F_SETFL, old_fnctl | O_NONBLOCK);
    if ( retval < 0) {
        perror("fnctl error");
    }
}

int parapipe(char *cmd, char *header, int njob, int job_nline) {
    int chunk_size = job_nline * njob;
    gstr_t chunk[chunk_size];
    memset(chunk, 0, chunk_size * sizeof(gstr_t));

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
    int nline;
    while((nline = readlines(chunk, chunk_size, stdin))>0) {
        int nj = (nline - 1)/ job_nline + 1;
        //_Pragma("omp parallel for")
        for (int j = 0; j < nj; j++) {
            struct job *job = &jobs[j];
            int end = (1+j) * job_nline;
            if (end > nline) end = nline;
            for (int i=j*job_nline; i<end; i++) {
                //write(job->fdw, chunk[i].s, chunk[i].l);
                fwrite(chunk[i].s, chunk[i].l, 1, job->fpw);
            }
            fflush(job->fpw);

            char buf[4096];
            int nread;
            while(1) {
                nread = read(job->fdr, buf, 4096);
                if (nread > 0) {
                    char *p = buf + nread;
                    while (--p >= buf) { if (*p != '\n') break;}
                    int remain  = 0;
                    if (p >= buf) { // with \n
                        fwrite_gstr_vec(job->redbuf, stdout);
                        fwrite(buf, p - buf + 1, 1, stdout);
                        fflush(stdout);
                        clean_gstr_vec(job->redbuf);
                    }
                    // number of chars after \n
                    remain = buf + nread - p - 1;
                    if (remain > 0) {
                        gstr_t *gs = calloc(1, sizeof(gstr_t));
                        gs->l = remain;
                        gs->s = malloc(remain);
                        memcpy(gs->s, buf, remain);
                        vec_push(job->redbuf, gs);
                    }
                } else break;
            }

        }
        for (int i=0; i < nline; i++) {
            gfree(chunk[i].s);
            chunk[i].l = 0;
        }
    } // finish writing

    // must close all write-ends, I dont know why
    for (int i=0; i<njob; i++) {
        close(jobs[i].fdw);
    }

    for (int i=0; i<njob; i++) {
        struct job *job = &jobs[i];
        char buf[4096];
        int nread = 0;
        while(1) {
            nread = read(job->fdr, buf, 4096);
            if (nread == 0) break;
            if (nread > 0) {
                int remain = nread;
                gstr_t *gs = calloc(1, sizeof(gstr_t));
                gs->l = remain;
                gs->s = malloc(remain);
                memcpy(gs->s, buf, remain);
                vec_push(job->redbuf, gs);
            };
        }
        fwrite_gstr_vec(job->redbuf, stdout);
        fflush(stdout);
        destroy_gstr_vec(&job->redbuf);

        close(job->fdr);
        //int status; waitpid(job->pid, &status, 0);
    }

    return 0;
}
