#include "gstring.h"
#include "parapipe.h"
#include "lopt.h"
#include <getopt.h>
#include <omp.h>
#include <stdlib.h>
#include "vec.h"
typedef vec_t(gstr_t) gvec_t;

int main(int argc, char*argv[]) {
    if (argc < 2) {
        printf(
                "Usage: echo asdf | parapipe \"wc\"\n\
                Arguments:\n\
                -j the number of jobs.\n\
                -h the number of header, will repeat for each job.\n\
                -r the number of lines per record, default 1.\n\
                Notice: The input orders are not guaranteed\n.");
        exit(11);
    }
    
    struct config {
        char *cmd;
        int njob;
        int ispipe;
        int header;
        int record_nline;
    };

    struct config config;
    memset(&config, 0, sizeof(struct config));
    config.njob = 2;
    config.header = 0;
    config.ispipe = 0;
    config.record_nline = 1;
    char *header = NULL;
    gvec_t *header_vec = calloc(1, sizeof(gvec_t));
    config.cmd = argv[argc-1];
    argc-=2; argv++;

    gstr_t remain = {0, NULL};
    int c;
    if ((c = lgetopt(argc, argv, "-j", 1)) > 0) {
        config.njob = atoi(argv[c]);
        if (config.njob < 1) {
            fprintf(stderr, "error: the number of jobs must be larger than zero.\n");
            exit(11);
        }
    }
    if ((c = lgetopt(argc, argv, "-r", 1)) > 0) {
        config.record_nline = atoi(argv[c]);
        if (config.record_nline < 1) {
            fprintf(stderr, "error: the number of lines per record must be larger than zero.\n");
            exit(11);
        }
    }
    if ((c = lgetopt(argc, argv, "-h", 1)) > 0) {
        if (argv[c] != NULL) {
            if (argv[c][0] == '^') {
                if (strlen(argv[c]) < 2) {
                    fprintf(stderr, "error: not found starting string.\n");
                    exit(11);
                }
                while (1) {
                    int n = readlines(&remain, 1, stdin);
                    if (n != 1 || gstrstartwith(remain, (gstr_t) {strlen(argv[c]), argv[c]+1})<0) {
                        break;
                    } else {
                        vec_push(header_vec, remain);
                        remain.l = 0; remain.s = NULL;
                    }
                }
            } else {
                config.header = atoi(argv[c]);
                if (config.header < 1) {
                    fprintf(stderr, "error: the number of header must be larger than zero.\n");
                    exit(11);
                }
            }
        } else {
            config.header = 1;
        }
    }

    // printf("%s\n", config.cmd);
    if (config.header > 0) {
        gstr_t lines[config.header];
        int nline = readlines(lines, config.header, stdin);
        if (nline == config.header) {
            int len  = 0;
            for (int i=0; i<nline; i++) {
                len += lines[i].l;
            }
            header = calloc(len+1, 1);
            len = 0;
            for (int i=0; i<nline; i++) {
                memcpy(header + len, lines[i].s, lines[i].l);
                len += lines[i].l;
            }
        }
    }

    if (header_vec->length > 0) {
        assert(header == NULL);
        int len = 0;
        for (int i=0; i<header_vec->length; i++) {
            len += header_vec->data[i].l;
        }
        header = calloc(len+1, 1);
        len = 0;
        for (int i=0; i<header_vec->length; i++) {
            memcpy(header + len, header_vec->data[i].s, header_vec->data[i].l);
            len += header_vec->data[i].l;
            gfree(header_vec->data[i].s);
        }
        gfree(header_vec->data);
        gfree(header_vec);
    }

    omp_set_num_threads(config.njob);
    parapipe(config.cmd, header, config.njob, remain, config.record_nline);

    gfree(header);
    return 0;
}
