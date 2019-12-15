#!/bin/bash
bin/scripts/cluster --command="rm /tmp/lowlatency*; cd src; sudo make clean; sudo make -j"
