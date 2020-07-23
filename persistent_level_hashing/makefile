all: plevel

plevel: test.o level_hashing.o hash.o pflush.o log.o
	gcc -o plevel test.o level_hashing.o hash.o pflush.o log.o-lm

hash.o: hash.c hash.h
	gcc -c hash.c

log.o: log.c log.h
	gcc -c log.c	

level_hashing.o: level_hashing.c level_hashing.h
	gcc -c level_hashing.c

test.o: test.c level_hashing.h
	gcc -c test.c

pflush.o: pflush.c pflush.h
	gcc -c pflush.c



clean:
	rm -rf *.o plevel
