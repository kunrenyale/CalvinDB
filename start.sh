#!/bin/bash
bin/scripts/cluster --command="start" --config calvin.conf --lowlatency=1 --type=0 --experiment=2  --percent_mp=0  --percent_mr=0  --hot_records=10000 --max_batch_size=100
