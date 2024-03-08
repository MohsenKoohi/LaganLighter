GCC := gcc
GXX := g++
LIB := poplar/lib64:$(LD_LIBRARY_PATH)

UP_GCC_DIR := ~/gcc9.2
ifneq ("$(wildcard $(UP_GCC_DIR)/bin/gcc)","")
	GCC := $(UP_GCC_DIR)/bin/gcc
	GXX := $(UP_GCC_DIR)/bin/g++
	LIB := $(UP_GCC_DIR)/lib64:$(LIB)
endif

OBJ := obj
LIB := $(LIB):~/Libs/numactl-2.0.1/usr/local/lib:~/papi6/lib 
INCLUDE_LIBS := $(addprefix -L , $(subst :, ,$(LIB))) 
INCLUDE_HEADER := $(addprefix -I , $(subst :,/../include ,$(LIB)))
FLAGS :=  -Wfatal-errors -lm -fopenmp -lpapi -lnuma -lpoplar -lrt

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
	@if [ `$(GCC) -dumpversion | cut -f1 -d.` -le 8 ]; then\
		$(GCC) -dumpversion; \
		echo -e "\033[0;33mError:\033[0;37m Version 9 or newer is required for gcc.\n\n";\
		exit -1;\
	fi

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
	@echo -e "graph_type: "$(graph_type)
	@echo -e "other args: "$(args)
	POPLAR_LIB_FOLDER=poplar/lib64 LD_LIBRARY_PATH=$(LIB) $(OMP_VARS) $(OBJ)/alg$*.o $(graph) $(graph_type) $(args)

all: poplar Makefile
	
poplar: FORCE
	make -C poplar all
	@if [ ! -f data/cnr-2000.graph ]; then \
		echo -e "--------------------\n\033[1;34mDownloading cnr-2000\033[0;37m"; \
		wget -P data "http://data.law.di.unimi.it/webdata/cnr-2000/cnr-2000.graph"; \
		wget -P data "http://data.law.di.unimi.it/webdata/cnr-2000/cnr-2000.properties"; \
		echo -e "--------------------\n";\
	fi

tests:
	make $(subst .c,,$(shell ls alg*.c))
	
clean:
	rm -f $(OBJ)/*.obj $(OBJ)/*.o
	touch *.c 
	
touch:
	touch *.c 

FORCE: ;

.SUFFIXES:

.keep_exec_files: $(addprefix $(OBJ)/,$(subst .c,.obj, $(shell ls *.c)))
