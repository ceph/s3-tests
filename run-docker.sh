#!/bin/sh

if [ -n $NOSETESTS_ATTR ]
then
    exec ./virtualenv/bin/nosetests -a $NOSETESTS_ATTR
else
    exec ./virtualenv/bin/nosetests
fi
