#include "gstring.h"
#include "parapipe.h"
#include "ketopt.h"
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
Notice: The input orders are not guaranteed\n.");
        exit(11);
    }
    struct config {
        char *cmd;
        int njob;
        int ispipe;
        int header;
    };

    struct config config;
    memset(&config, 0, sizeof(struct config));
    config.njob = 2;
    config.header = 0;
    config.ispipe = 0;
    static ko_longopt_t longopts[] = {
        {"pipe", ko_no_argument, 301},
        {NULL, 0, 0}
    };
    ketopt_t opt = KETOPT_INIT;
    int c;
    char *header = NULL;
    gvec_t *header_vec = calloc(1, sizeof(gvec_t));

    gstr_t remain = {0, NULL};
    while ((c = ketopt(&opt, argc, argv, 1, "j:h:", longopts)) >= 0) {
        if (c == 'j') {
            config.njob = atoi(opt.arg);
            if (config.njob < 1) {
                fprintf(stderr, "error: the number of jobs must be larger than zero.\n");
                exit(11);
            }
        } else if (c  == 301) {
            config.ispipe = 1;
        } else if (c  == 'h') {
            if (opt.arg != NULL) {
                if (opt.arg[0] == '^') {
                    if (strlen(opt.arg) < 2) {
                        fprintf(stderr, "error: not found starting string.\n");
                        exit(11);
                    }
                    while (1) {
                        int n = readlines(&remain, 1, stdin);
                        if (n != 1 || gstrstartwith(remain, (gstr_t) {strlen(opt.arg+1), opt.arg+1})<0) {
                            break;
                        } else {
                            vec_push(header_vec, remain);
                            remain.l = 0; remain.s = NULL;
                        }
                    }
                } else {
                    config.header = atoi(opt.arg);
                    if (config.header < 1) {
                        fprintf(stderr, "error: the number of header must be larger than zero.\n");
                        exit(11);
                    }
                }
            } else {
                config.header = 1;
            }
        }
    }
    for (int i=opt.ind+1; i<argc; i++){
        *(argv[i]-1) = ' ';
    }
    config.cmd = argv[opt.ind];

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
    parapipe(config.cmd, header, config.njob, remain);

    gfree(header);
    return 0;
}

