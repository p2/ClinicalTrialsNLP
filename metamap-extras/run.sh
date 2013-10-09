#!/bin/bash

BINDIR=$(dirname $0)/bin
METAMAP=$BINDIR/metamap13

# set the run directory
if [ 'xx' = "xx$1" ]; then
	echo "Provide the run directory as first call argument" >&2
	exit 1
elif [ -d $1 ]; then
	RUN=$1
else
	echo "The run directory $1 does not exist" >&2
	exit 1
fi

# check for executables
if [ ! -f "$METAMAP" ]; then
	echo "The MetaMap executable is not present at $METAMAP, did you run the install script? BINDIR: $BINDIR" >&2
	exit 1
fi
if [ ! -f "$BINDIR/wsdserverctl" ]; then
	echo "The MetaMap server executable is not present at bin/wsdserverctl, did you run the install script?" >&2
	exit 1
fi

# check for input files
if [ ! -d "$RUN/metamap_input" ]; then
	echo "There is no directory \"metamap_input\" in the run directory \"$RUN\"" >&2
	exit 1
fi
if [ ! -d "$RUN/metamap_output" ]; then
	mkdir "$RUN/metamap_output"
	if [ 0 -ne $? ]; then
		echo "Failed to create MetaMap output directory" >&2
		exit 1
	fi
fi

# start servers if they are not running
if [ $(ps -ax | grep WSD_Server | wc -l) -lt 2 ]; then
	$BINDIR/wsdserverctl start
	if [ 0 -ne $? ]; then
		echo "Failed to start WSD Server" >&2
		exit 1
	fi
fi
if [ $(ps -ax | grep MedPost-SKR | wc -l) -lt 2 ]; then
	$BINDIR/skrmedpostctl start
	if [ 0 -ne $? ]; then
		echo "Failed to start SKR" >&2
		exit 1
	fi
fi

# run it
for f in "$RUN/metamap_input/"*; do
	out=$(echo $f | sed s/_input/_output/)
	# $METAMAP --XMLf "$f" "$out"		# this shit does not work!!!
	# the only way it works is by piping echo!!! WHO DOES THIS???
	echo $(cat "$f") | "$METAMAP" --XMLf --silent | awk "NR>1" >"$out"
done

exit 0
