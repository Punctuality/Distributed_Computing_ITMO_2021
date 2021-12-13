
.PHONY: all clean

all: a.out

clean:
	rm a.out

a.out: main.c
	$(CC) -o $@ -std=c99 -Wall -pedantic -g -O0 $< $(CFLAGS) # -fsanitize=address

%.c: