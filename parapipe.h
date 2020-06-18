#ifndef PARAKIT_H_79
#define PARAKIT_H_79 1

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "gstring.h"
struct pp_config {
    char *cmd;
    int njob;
    int ispipe;
    int header_n;
    char *header_s;
    int in_record_nline;
    int out_record_nline;
};

#define gfree(x) do {if(x != NULL){free(x); x = NULL;}} while(0)
int parapipe(struct pp_config*, gstr_t remain);

ssize_t getline(char **, size_t*, FILE *);

static inline int readlines(gstr_t *ret, int capacity, FILE *fp) {
    char *line = NULL;
    size_t len = 0;
    ssize_t nread = -1; 
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

#endif
