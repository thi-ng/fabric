#!/bin/sh

readonly MODULES="core facts"
FILES="README.org"

for m in $MODULES; do
    MODULE="fabric-$m"
    SRC="$MODULE/README.org $MODULE/src/*.org $MODULE/test/*.org $MODULE/bench/*.org"
    rm -rf $MODULE/babel/src $MODULE/babel/test
    for f in `ls $SRC`; do
        FILES="$FILES $f"
    done
done

#echo $FILES
./tangle.sh $FILES
