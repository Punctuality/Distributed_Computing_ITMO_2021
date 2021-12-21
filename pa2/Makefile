.PHONY: all clean run

all: a.out

run_context: all
	export LD_LIBRARY_PATH="$(shell pwd -P):$LD_LIBRARY_PATH"
	export LD_PRELOAD="$(shell pwd -P)/libruntime.so"

run: run_context
	./a.out -p 2 10 20

clean:
	rm -f a.out
	rm -f events.log_msg
	rm -f pipes.log_msg

a.out: pa23.c bank_robbery.c
	$(CC) -o $@ -std=c99 -Wall -pedantic -g -O0 $^ $(CFLAGS) -L. -lruntime -fsanitize=address

%.c:

