#ifndef _LGETOPT_H_79
#define _LGETOPT_H_79 1
#include <string.h>
#include <stdio.h>

int lgetopt(int argc, char *argv[], char* o, int required) {
    if (argc < 1) return -1;
    int i = 0;
    for (; i<argc && strcmp(o, argv[i])!=0; i++) {}
    if (i == argc) {
        return -1;
    }
    ++i;
    if (required == 0)  return i;
    if ( i == argc || (strlen(argv[i])>1 && *argv[i] == '-')) {
        return -1;
    }
    return i;
}

#endif
