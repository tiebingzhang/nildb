# http://creativecommons.org/publicdomain/zero/1.0/

all:
	gcc -Wall -O2 -DNILDB_TEST -o nildb-test nildb.c -s

clean:
	rm -f nildb-test *.o test.db
