#!/bin/bash
#
# This script runs an algorithm for a number of graph datasets

# Print help
	echo -e "\n\033[1;35mLaganLighter Launcher Script\033[0;37m\n"
	echo "Arguments to be passed to this script:"
	echo "  alg=algorithm-filename (with or without .c)"
	echo "  df=path/to/datasets/folder (default ./data)"
	echo "  args=\"additional arguemnts to be passed to the program and separated by space and put in double quotes\" (default empty)"
	echo "  -iw (include weighted graphs, default: 0)"
	echo "  -s1 (stop after first dataset, default: 0)"
	echo "  -shm-store (store graphs in shm, default: 0)"
	echo "  -shm-delete (delete shm graphs at the end, default: 0)"
	echo

# Reading arguments
	echo -e "\033[0;34mArguments\033[0;37m"
	ALG=`echo $@ | grep -oP "(alg=?[\S]*)"`
	if [ `echo $ALG | grep -oc "="` -eq 1 ]; then
		ALG=`echo $ALG | cut -f2 -d=`;
	fi
	if [ `echo $ALG | grep -oc "\.c"` -eq 1 ]; then
		ALG=`echo $ALG | cut -f1 -d.`;
	fi
	if [ ! -e "$ALG.c" ]; then 
		echo "File \"$ALG.c\" does not exist.";
		exit 1;
	fi
	echo "Algorithm: $ALG"

	DF="./data"
	ARGS=""
	IW=0
	S1=0
	SHM_STORE=0
	SHM_DELETE=0

	for i in `seq 1 $#`; do
		if [[ "${!i}" == *"df"* ]]; then 
			DF=`echo "${!i}" | cut -f2 -d=`
		fi

		if [[ "${!i}" == *"args"* ]]; then 
			ARGS=`echo "${!i}" | cut -f2 -d=`
		fi

		if [ "${!i}" == "-iw" ]; then
			IW=1
		fi

		if [ "${!i}" == "-s1" ]; then
			S1=1
		fi

		if [ "${!i}" == "-shm-store" ]; then
			SHM_STORE=1
		fi

		if [ "${!i}" == "-shm-delete" ]; then
			SHM_DELETE=1
		fi
	done 

	if [ ! -e $DF ]; 
	then
		echo "Datasets folder \"$DF\" does not exist.";
		exit 2;
	fi;
	
	echo "Additional args (arg): \"$ARGS\""
	echo "Datasets folder (df): $DF" 
	echo "Include weighted graphs (-iw): $IW"
	echo "Stop after first dataset (-s1): $S1"
	echo "Store graph in shm (-shm-store): $SHM_STORE"
	echo "Delete shm graphs at end (-shm-delete): $SHM_DELETE"
	echo 

# Identifying datasets
	echo -e "\033[0;34mDatasets\033[0;37m"
	dataset_patterns="*.graph *.txt"
	if [ $IW == 1 ]; then
		dataset_patterns="$dataset_patterns *.labels"
	fi

	initial_datasets=(`(cd $DF; ls $dataset_patterns)`)
	datasets=""
	dataset_edges=""
	for (( i=0; i <  ${#initial_datasets[@]}; i++)); do
		ds=${initial_datasets[$i]}
		sf=`echo $ds | rev | cut -f1 -d. | rev`
		edges_count=0
		
		if [ $sf == "txt" ]; then
			edges_count=`head "$DF/$ds" -n2 | tail -n1`
		fi

		if [ $sf == "graph" ]; then
			prop_file=`echo $ds | sed 's/\(.*\)\.graph/\1\.properties/'`
			edges_count=`cat "$DF/$prop_file" | grep -P "\barcs[\s]*=" | cut -f2 -d= | xargs`
		fi

		if [ $sf == "labels" ]; then
			laebls_prop_file=`echo $ds | sed 's/\(.*\)\.labels/\1\.properties/'`
			prop_file=`cat "$DF/$laebls_prop_file" | grep -P "underlyinggraph[\s]*=" | cut -f2 -d=|xargs`".properties"
			edges_count=`cat "$DF/$prop_file" | grep -P "\\barcs=" | cut -f2 -d=`
		fi

		if [ `echo $edges_count |  tr -cd ' \t' | wc -c ` != 0 ]; then
			edges_count=0;
		fi

		if [ $edges_count == "0" ]; then
			echo "Dataset \"$DF/$ds\" did not recognize."
			continue;
		fi

		datasets="$datasets $ds"
		dataset_edges="$dataset_edges $edges_count"

		# echo $i $ds $sf $edges_count;
	done	

	datasets=($datasets);
	dataset_edges=($dataset_edges);

	sorted_datasets=`for (( i=0; i < ${#datasets[@]}; i++)); do
		echo ${datasets[$i]} ${dataset_edges[$i]}
	done | sort -n -k2`

	datasets=""
	while IFS= read -r line; do
		ds=`echo $line|cut -f1 -d' '`
		ec=`echo $line|cut -f2 -d' '`
		echo "$ds, |E|: $ec" 
		datasets="$datasets $ds"
	done <<< "$sorted_datasets"

# Processing graphs
	echo -e "\n\033[0;34mProcessing Datasets\033[0;37m"
	for ds in $datasets; do
		echo $ds;
	done

# Post processing actions 
	if [ $SHM_DELETE == 1 ]; then 
		make clean_shm_graphs
	fi

echo 