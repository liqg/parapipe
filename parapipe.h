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

#define gfree(x) do {if(x != NULL){free(x); x = NULL;}} while(0)

int parapipe(char *cmd, char *header, int njob, int job_nline);
int readlines(gstr_t *ret, int capacity, FILE *fp);

#endif
