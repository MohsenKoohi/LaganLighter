GCC_DIR := ~/gcc9.2

OBJ := obj
GCC := $(GCC_DIR)/bin/gcc 
GXX := $(GCC_DIR)/bin/g++ 
LIB := $(GCC_DIR)/lib64:~/Libs/numactl-2.0.1/usr/local/lib:~/papi6/lib:$(LD_LIBRARY_PATH)
INCLUDE_LIBS := $(addprefix -L , $(subst :, ,$(LIB)))
INCLUDE_HEADER := $(addprefix -I , $(subst :,/../include ,$(LIB))) 
FLAGS :=  -Wfatal-errors -lm -fopenmp -lpapi -lrt -lnuma 

_threads_per_core := $(shell lscpu | grep "Thread(s) per core" | head -n1 | cut -f2 -d":"|xargs)
_total_threads := $(shell nproc --all) #getconf _NPROCESSORS_ONLN
_total_cores := $(shell echo "$(_total_threads)/$(_threads_per_core)"|bc)
_available_threads := $(shell nproc)
_available_cores := $(shell echo "$(_available_threads)/$(_threads_per_core)"|bc)

# _order := {$(shell numactl --hardware | grep cpus | cut -f2 -d":" | xargs | sed -e "s/ /},{/g")}

ifdef no_ht
	# without hyper-threading
	OMP_VARS :=  OMP_PLACES={0}:$(_total_cores):1 OMP_PROC_BIND=close OMP_DYNAMIC=false OMP_WAIT_POLICY=active OMP_NUM_THREADS=$(_available_cores)
else
	# with hyper-threading
	OMP_VARS :=  OMP_PLACES=threads OMP_PROC_BIND=close OMP_DYNAMIC=false OMP_WAIT_POLICY=active OMP_NUM_THREADS=$(_available_threads)
endif

ifdef debug
	COMPILE_TYPE := -g
else
	COMPILE_TYPE := -O3 # -DNDEBUG
endif

$(OBJ)/alg%.obj: alg%.c *.c Makefile poplar
	@echo Creating $@
	$(GCC) $(INCLUDE_HEADER) $(FLAGS) $< -std=gnu11  $(COMPILE_TYPE) -c -o $@ 
	@echo 

alg%: $(OBJ)/alg%.obj *.c Makefile
	@echo Building $@ 
	$(GCC) $(INCLUDE_LIBS) $(OBJ)/$@.obj $(FLAGS) -o $(OBJ)/alg$*.o
	@echo ""
	@echo -e "\nExecuting $@"
	@echo -e "#threads_per_core: "$(_threads_per_core)
	@echo -e "#total_cores: "$(_total_cores)
	@echo -e "#total_threads: "$(_total_threads)
	@echo -e "#available_cores: "$(_available_cores)
	@echo -e "#available_threads: "$(_available_threads) 
	@echo -e "graph: "$(graph)
	@echo -e "other args: "$(args)
	LD_LIBRARY_PATH=$(LIB) $(OMP_VARS) $(OBJ)/alg$*.o $(graph) $(args)

all: poplar Makefile
	
poplar: FORCE
	make -C poplar all

clean:
	rm -f $(OBJ)/*.obj $(OBJ)/*.o
	touch *.c 
	
touch:
	touch *.c 

FORCE: ;

.SUFFIXES:

.keep_exec_files: $(addprefix $(OBJ)/,$(subst .c,.obj, $(shell ls *.c)))
