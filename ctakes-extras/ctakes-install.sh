#!/bin/bash
#
# Download and install cTAKES
#
# Dependencies:
# - java
# - mvn (Maven)
# - svn (Subversion)

PWD=$(pwd)
ORIG=$(echo $PWD/$(dirname $0) | sed 's#/\.##')
SOURCE="$ORIG/../../ctakes-svn"
TARGET="$SOURCE/../ctakes"
echo "->  Subversion repo:   $SOURCE"
echo "->  Install directory: $TARGET"

# warn if we already have an install
if [ -d "$TARGET" ]; then
	echo "x>  Install directory already exists, you must remove it manually before we can install cTAKES anew."
	exit 1
fi

# move to project root (should not hardcode this...)
cd $SOURCE/..

# checkout ctakes from SVN
if [ ! -d "$SOURCE" ]; then
	echo "->  Checking out cTAKES from Subversion repo"
	# svn co https://svn.apache.org/repos/asf/ctakes/trunk "$SOURCE"
	out_svn=$(svn co https://svn.apache.org/repos/asf/ctakes/tags/ctakes-3.1.0 "$SOURCE")
else
	echo "->  Updating cTAKES repo"
	cd "$SOURCE"
	out_svn=$(svn up)
	cd ..
fi

if [ 1 == $? ]; then
	echo "x>  Failed to checkout cTAKES, here's Subversion's output:"
	echo $out_svn
	exit 1
fi

# package with Maven
echo "->  Packaging cTAKES"
cd "$SOURCE"
out_mvn=$(mvn package)

if [ 1 == $? ]; then
	echo "x>  Failed to package cTAKES, here's Maven's output:"
	echo $out_mvn
	exit 1
fi

# extract and move built products into place
echo "->  Moving cTAKES into place"
base=$(echo ctakes-distribution/target/*-bin.tar.gz)
tar xzf $base
mv $(basename ${base%-bin.tar.gz}) "$TARGET"

# download resources
cd "$TARGET"
if [ -z ctakes-resources.zip ]; then
	echo "->  Downloading cTAKES resources"
	curl -o ctakes-resources.zip "http://hivelocity.dl.sourceforge.net/project/ctakesresources/ctakes-resources-3.1.0.zip"
	tar xzf ctakes-resources.zip
fi

# add special classes and scripts
echo "->  Adding special files"
cp "$ORIG/run.sh" "$TARGET/"
cp "$ORIG"/*.class "$TARGET/"
cp "$ORIG"/*.java "$TARGET/"
cp "$ORIG"/*.xml "$TARGET/"

echo "->  Done"
