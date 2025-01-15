#!/bin/bash

TPL_FILE=l3_rwa.tpl.c
TARGET_FILE=l3_rwa.c

if [ ! -f $TPL_FILE ]; then
   if [ -f "benchmarks/$TPL_FILE" ]; then
      TPL_FILE="benchmarks/"$TPL_FILE
      TARGET_FILE="benchmarks/"$TARGET_FILE
   else
      echo "Could not find the TPL_FILE : $TPL_FILE"
      echo
      exit 1;
   fi
fi

echo -e "\nCreating $TARGET_FILE from $TPL_FILE"

skiplines=1
cat $TPL_FILE | while read line
do
   echo $line
   if [ `echo $line | grep "//" -c` == 1 ]; then
      skiplines=`echo "$skiplines + 1"|bc`
      echo $skiplines >__skiplines_temp.txt
   else
      break;
   fi
done 
skiplines=`cat __skiplines_temp.txt`
rm __skiplines_temp.txt
echo "Lines to be skipped: "$skiplines

tail -n +$skiplines $TPL_FILE > __tpl_temp.txt

echo -e "// This file has been created by l3_rwa_builder.sh and based on $TPL_FILE\n" > $TARGET_FILE
echo -e "#ifndef __L3_BENCHMARK\n#define __L3_BENCHMARK" >> $TARGET_FILE

cat $TPL_FILE | grep '^//\$' | while read line
do
   name=`echo $line | cut -d$ -f2`
   instruction=`echo $line | cut -d$ -f3`
   sed -e 's/FUNC_NAME/'"$name"'/g' -e 's/BENCHMARK_INSTRUCTIONS/'"$instruction"'/g' __tpl_temp.txt >> $TARGET_FILE
done 

rm __tpl_temp.txt

echo -e "#endif\n" >> $TARGET_FILE

echo -e "Creating $TARGET_FILE completed.\n"