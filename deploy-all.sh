#!/bin/bash

readonly PROGNAME=$(basename $0)
readonly PROGDIR=$(cd "$(dirname "$0")"; pwd)
readonly ARGS="$@"
readonly PREFIX="deploy-"

main() {
    local modules="core facts"

    for m in $modules
    do
        echo "deploying module $PREFIX$m..."
        echo "----------------------------------------"
        cd $PROGDIR/$PREFIX$m/babel
        lein deploy clojars
    done

    cd $PROGDIR/babel
    lein deploy clojars
}

main
