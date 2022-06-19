#!/bin/sh
mkdir -p ../build;
cd ../build;

for args in $@
do
     param="$param $args";
done
echo "options:  $param";
cmake .. $param;

make clformat;
make wukong_cpplint;
cd ../scripts;
