#!/usr/bin/env bash

function restart()
{
    stop
    for i in {3..1}
    do
        echo -n "$i "
        sleep 1
    done
    echo 0
    start
}

restart
