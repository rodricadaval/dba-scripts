#!/bin/bash
source ~/dba-scripts/virtual-env/bin/activate
if [ -z "$4" ]; then
	python3 ~/dba-scripts/mongodb/common/downscaling_rr.py --account $1 --region $2 --$3
else
	python3 ~/dba-scripts/mongodb/common/downscaling_rr.py --account $1 --region $2 --$3 --tags $4
fi
deactivate


