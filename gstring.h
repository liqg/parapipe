#ifndef GSTRING_H_79
#define GSTRING_H_79

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>

#ifdef _cplusplus
extern "C" {
#endif

typedef struct _gstr_t {
    int64_t l; //using int is more convenient than int32_t for funcs such as firstof move
    char *s;
} gstr_t;

// return gstr_t, calloc when l>0
static inline gstr_t gstr_init(int64_t l) {
    gstr_t gs = (gstr_t) {0, NULL};
    if (l > 0) {
        gs.l = l;
        gs.s = calloc(1, l);
    }
    return gs;
}

//return point version, alloc both
static inline void *gstr_calloc(int64_t l) {
    gstr_t *gs = calloc(1, sizeof(gstr_t));
    if (l > 0) {
        gs->l = l;
        gs->s = calloc(1, l);
        assert(gs->s);
    }
    return gs;
}

static inline int gstrfirstc (const gstr_t s, const char c){
    int ret = -1;
    for(int64_t i=0; i < s.l; i++){
        if(s.s[i] == c){
            ret = i;
            break;
        }
    }
    return ret;
}

static inline void gstrmove (gstr_t *s, const int64_t n){
    s->l = s->l - n;
    s->s = s->s + n;
}

// 0 for match
static inline int gstrcmp(const gstr_t s1, const gstr_t s2){
    int ret = 0;
    if(s1.l > s2.l) ret = 1;
    if(s1.l < s2.l) ret = -1;

    if(ret == 0) {
        for(int64_t i = 0; i < s1.l; i++){
            ret = s1.s[i] - s2.s[i];
            if(ret != 0) break;
        }
    }
    return ret;
}

static inline int gstrstartwith(const gstr_t s1, const gstr_t s2){
    int ret = 1;
    if (s2.l <= s1.l && s2.l > 0) {
        for (int64_t i = 0; i < s2.l; i++) {
           if (s1.s[i] != s2.s[i]){
               ret = -1;
               break;
           }
        } 
    } else ret = -1;
    return ret;
}

// remove consecutive chars in head and tail
static inline void gstrclean (gstr_t *s, const char c){
    while(s->l > 0 && s->s[0] == c){
        gstrmove(s, 1);
    }
    while(s->l > 0 && s->s[s->l - 1] == c){
        s->l--;
    }
}

static inline gstr_t gsubstr(const gstr_t s, const int64_t start, const int64_t end){
    gstr_t gs = (gstr_t) {0, 0};
    gs.l = end - start + 1;
    gs.s = s.s + start;
    return gs;
}

// not forget to free str 
static inline gstr_t int_to_gstr(const long x){
    int n = snprintf(NULL, 0, "%ld", x );
    gstr_t gs;
    gs.s = (char *)calloc(n+1, 1);
    gs.l = n;
    snprintf(gs.s, n+1, "%ld", x);
    return gs;
}

static inline gstr_t double_to_gstr(const double x){
    int n = snprintf(NULL, 0, "%g", x );
    gstr_t gs;
    gs.s = (char*) calloc(n+1, 1);
    gs.l = n;
    snprintf(gs.s, n+1, "%g", x);
    return gs;
}

// return a null-terminated copy, not forget to free
static inline void gstr_to_cstr(char *buf, const gstr_t s){
    memcpy(buf, s.s, s.l);
    buf[s.l] = 0;
}

static inline int32_t gstr_to_int32 (const gstr_t s) {
    char tmp[s.l + 1];
    tmp[s.l] = '\0';
    memcpy(tmp, s.s, s.l);
    int64_t ret =  sizeof(int) == 4 ? atoi(tmp) : atol(tmp);
    return ret;
}

static inline int64_t gstr_to_int64 (const gstr_t s) {
    char tmp[s.l + 1];
    tmp[s.l] = '\0';
    memcpy(tmp, s.s, s.l);
    int64_t ret =  sizeof(long) == 8 ? atol(tmp) : atoll(tmp);
    return ret;
}

static inline double gstr_to_double (const gstr_t s) {
    char tmp[s.l + 1];
    tmp[s.l] = '\0';
    memcpy(tmp, s.s, s.l);
    double ret =  atof(tmp);
    return ret;
}

static inline float gstr_to_float (const gstr_t s) {
    char tmp[s.l + 1];
    tmp[s.l] = '\0';
    memcpy(tmp, s.s, s.l);
    double ret =  atof(tmp);
    return (float) ret;
}
// not forget to free
static inline gstr_t memcat (const void **s, const int64_t *l, const int64_t n){
    int64_t total = 0;
    for (int64_t i = 0; i < n; i++) {
        total += l[i];
    }
    char *p = (char*) malloc(total);
    gstr_t ret = (gstr_t) {total, p};

    for (int64_t i = 0; i < n && s[i] != NULL; i++) {
        memcpy(p, s[i], l[i]);
        p += l[i];
    }
    return ret;
}

//merge gstr point array
static inline gstr_t gstr_merge(const gstr_t **gs, const int64_t n){
    int64_t total = 0;
    for (int64_t i=0; i <n; i++) total+=gs[i]->l;
    char *p = (char*) malloc(total+1);
    assert(p);
    gstr_t ret = (gstr_t) {total, p};
    for (int64_t i=0; i<n; i++){
        if (gs[i]->l > 0) {
            memcpy(p, gs[i]->s, gs[i]->l);
            p += gs[i]->l;
        }
    }
    ret.s[total] = '\0';
    return ret;
}

//merge gstr array
static inline gstr_t gstr_merge2(const gstr_t *gs, const int64_t n){
    int64_t total = 0;
    for (int64_t i=0; i <n; i++) total+=gs[i].l;
    char *p = (char*) malloc(total+1);
    assert(p);
    gstr_t ret = (gstr_t) {total, p};
    for (int64_t i=0; i<n; i++){
        if (gs[i].l > 0) {
            memcpy(p, gs[i].s, gs[i].l);
            p += gs[i].l;
        }
    }
    ret.s[total] = '\0';
    return ret;
}

static inline gstr_t gstr_joint(const gstr_t **gs, const int64_t n, char sep){
    if (sep < 0) return gstr_merge(gs, n);
    int64_t total = n;
    for (int64_t i=0; i<n; i++) total+=gs[i]->l;
    char *p = (char*) malloc(total);
    assert(p);
    gstr_t ret = (gstr_t) {total-1, p};
    for (int64_t i=0; i<n; i++){
        if (gs[i]->l > 0) {
            memcpy(p, gs[i]->s, gs[i]->l);
            p += gs[i]->l;
            *(p++) = sep;
        }
    }
    ret.s[total-1] = '\0';
    return ret;
}

static inline gstr_t gstr_joint2(const gstr_t *gs, const int64_t n, char sep){
    if (sep < 0) return gstr_merge2(gs, n);
    int64_t total = n;
    for(int64_t i=0; i<n; i++) total += gs[i].l;
    char *p = (char*) malloc(total);
    assert(p);
    gstr_t ret = (gstr_t) {total-1, p};
    for (int64_t i=0; i<n; i++){
        if (gs[i].l > 0) {
            memcpy(p, gs[i].s, gs[i].l);
            p += gs[i].l;
            *(p++) = sep;
        }
    }
    ret.s[total-1] = '\0';
    return ret;
}

static inline gstr_t gstr_paste(const gstr_t gs1, const gstr_t gs2) {
    const gstr_t *gs[2];
    gs[0]=&gs1, gs[1] = &gs2;
    return gstr_merge(gs, 2);
}


#ifdef  __cplusplus
}
#endif

#endif
