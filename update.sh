#!/bin/bash
rm /tmp/lowlatency*
bin/scripts/cluster --command="cd src; sudo make clean; sudo make -j"
