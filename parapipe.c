#include "parapipe.h"
#include <limits.h>
#include <linux/limits.h>

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


int parapipe(char *cmd, char *header, int njob, int job_nline) {
    struct job {
        FILE *fp;
    };

    FILE *fp = stdin;
    int chunk_size = job_nline * njob;

    gstr_t chunk[chunk_size];
    memset(chunk, 0, chunk_size * sizeof(gstr_t));

    struct job jobs[njob];
    memset(jobs, 0, sizeof(struct job));
    for (int i=0; i<njob; i++) {
        struct job* job = &jobs[i];
        FILE *fpout = popen(cmd, "w");
        if (fpout == NULL) {
            fprintf(stderr, "error: popen is failed.\n");
        }
        job->fp =  fpout;
        if (header != NULL) {
            fprintf(fpout, "%s", header);
            fflush(fpout);
        }
    }
    int nline;
    while((nline = readlines(chunk, chunk_size, fp))>0){
        int nj = (nline - 1)/ job_nline + 1;
        _Pragma("omp parallel for")
            for (int j = 0; j < nj; j++) {
                int end = (1+j) * job_nline;
                if (end > nline) end = nline;
                for (int i=j*job_nline; i<end; i++) {
                    fwrite(chunk[i].s, 1, chunk[i].l, jobs[j].fp);
                }
            }

        for (int i=0; i < nline; i++) {
            gfree(chunk[i].s);
            chunk[i].l = 0;
        }
    }

    for (int i=0; i<njob; i++) {
        pclose(jobs[i].fp);
    }
    return 0;
}
