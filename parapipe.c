#include "parapipe.h"
#include "gstring.h"

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

struct job {
    char *cmd;
    int  fdr;
    FILE *fpr;
    int  fdw;
    FILE *fpw;
};

extern char **environ;

void init_job(struct job *job, char *cmd) {
    job->cmd = cmd;
    int writepipe[2] = {-1,-1}, /* parent -> child */
        readpipe [2] = {-1,-1}; /* child -> parent */
    pid_t   childpid;

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
        char *argp[] = {"sh", "-c", NULL, NULL};
        argp[2] = cmd;
        execve("/bin/bash", argp, environ);
        _exit(127);

        /* do child stuff */
    }
    else                /* in the parent */
    {
        close(CHILD_READ);
        close(CHILD_WRITE);
        job->fpr = fdopen(PARENT_READ,  "r");
        job->fpw = fdopen(PARENT_WRITE, "w");
        job->fdr = PARENT_READ;
        job->fdw = PARENT_WRITE;
        //close(PARENT_READ);
        //close(PARENT_WRITE);
    }
}

int parapipe(char *cmd, char *header, int njob, int job_nline) {

    FILE *fp = stdin;
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
    while((nline = readlines(chunk, chunk_size, fp))>0){
        int nj = (nline - 1)/ job_nline + 1;
        _Pragma("omp parallel for")
            for (int j = 0; j < nj; j++) {
                struct job *job = &jobs[j];
                int end = (1+j) * job_nline;
                if (end > nline) end = nline;
                for (int i=j*job_nline; i<end; i++) {
                    fwrite(chunk[i].s, 1, chunk[i].l, job->fpw);
                }
           }

        for (int i=0; i < nline; i++) {
            gfree(chunk[i].s);
            chunk[i].l = 0;
        }
    }

    for (int i=0; i<njob; i++) {
        struct job *job=&jobs[i];
        char buf[111];
        char *l = NULL;
        size_t len;
        int nread = getline(&l, &len, job->fpr);
        for (int k=0; buf[k] != 0 && k<10; k++) {
            if (buf[k] == '\n') {break;
            }}
        if (nread > 0)
            printf("%s, %i", buf, nread);
        fclose(job->fpr);
        fclose(job->fpw);
        close(job->fdr);
        close(job->fdw);
    }
    return 0;
}
