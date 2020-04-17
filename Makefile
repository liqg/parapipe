.PHONY: default
default: all
CC=gcc
CFLAG=-g -O3 -fPIC -std=c99 -lz -lm -fopenmp
objs=parapipe.o

parapipe.o: parapipe.c parapipe.h
	$(CC) $(CFLAG) -o $@ -c parapipe.c

all: $(objs) main.c
	ar -crv libparapipe.a $(objs)
	$(CC) -shared -fPIC -o libparapipe.so $(objs)
	$(CC) main.c -o parapipe -L. -l:libparapipe.a $(CFLAG)

clean: 
	rm -f *.o *.so *.a parapipe 
