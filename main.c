#include "parapipe.h"
#include "ketopt.h"
#include <omp.h>

int main(int argc, char*argv[]) {
    struct config {
        char *cmd;
        int njob;
        int ispipe;
        int header;
        int job_nline;
    };

    struct config config;
    memset(&config, 0, sizeof(struct config));
    config.njob = 2;
    config.header = 0;
    config.ispipe = 0;
    config.job_nline = 10;
    static ko_longopt_t longopts[] = {
        {"pipe", ko_no_argument, 301},
        {"header", ko_optional_argument, 302},
        {NULL, 0, 0}
    };
    ketopt_t opt = KETOPT_INIT;
    int c;
    while ((c = ketopt(&opt, argc, argv, 1, "l:j:", longopts)) >= 0) {
        if (c == 'j') {
            config.njob = atoi(opt.arg);
            if (config.njob < 1) {
                fprintf(stderr, "error: the number of jobs must be larger than zero.\n");
                exit(11);
            }
        } else if (c  == 'l') {
            config.job_nline = atoi(opt.arg);
            if (config.job_nline < 1) {
                fprintf(stderr, "error: the number of lines of each job must be larger than zero.\n");
                exit(11);
            }
        } else if (c  == 301) {
            config.ispipe = 1;
        } else if (c  == 302) {
            if (opt.arg != NULL) {
                config.header = atoi(opt.arg);
                if (config.header < 1) {
                    fprintf(stderr, "error: the number of header must be larger than zero.\n");
                    exit(11);
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
    char *header = NULL;
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

    omp_set_num_threads(config.njob);
    parapipe(config.cmd, header, config.njob, config.job_nline);
    
    gfree(header);
    return 0;
}

