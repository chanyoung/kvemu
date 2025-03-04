#!/bin/bash
# Huaicheng <huaicheng@cs.uchicago.edu>
# Copy necessary scripts for running FEMU

FSD="../femu-scripts"

CPL=(pkgdep.sh femu-compile.sh femu-compile-debug.sh run-whitebox.sh run-blackbox.sh run-nossd.sh run-zns.sh pin.sh ftk run-pink.sh run-pink.debug.sh run-lksv3.sh run-lksv3.debug.sh extract.sh)

echo ""
echo "==> Copying following FEMU script to current directory:"
for f in "${CPL[@]}"
do
	if [[ ! -e $FSD/$f ]]; then
		echo "Make sure you are under build-femu/ directory!"
		exit
	fi
	cp -r $FSD/$f . && echo "    --> $f"
done
echo "Done!"
echo ""

