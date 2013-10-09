#!/bin/sh
#
# Requires JAVA JDK 1.6+

# source UMLS credentials
if [ -z ./umls.sh ]; then
  echo "You need to provide UMLS credentials in the file ./umls.sh" &>2
  exit 1
fi
. ./umls.sh

# only set CTAKES_HOME if not already set
[ -z "$CTAKES_HOME" ] && CTAKES_HOME=$(dirname $0)
cd $CTAKES_HOME

# launch
java -cp $CTAKES_HOME:$CTAKES_HOME/lib/*:$CTAKES_HOME/desc/:$CTAKES_HOME/resources/ \
	-Dlog4j.configuration=file:$CTAKES_HOME/config/log4j.xml \
	-Dctakes.umlsuser=$UMLS_USERNAME -Dctakes.umlspw=$UMLS_PASSWORD \
	-Xms512M -Xmx1024M \
	CmdLineCpeRunner FilesToXmi.xml
