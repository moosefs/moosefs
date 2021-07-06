#!/bin/bash

# Create amalgamated source file, prints to stdout

echo '/*

This source file is the amalgamated version of the original.
Please see github.com/willemt/raft for the original version.
'
git log | head -n1 | sed 's/commit/HEAD commit:/g'
echo '
'
cat LICENSE
echo '
*/
'

echo '
#ifndef RAFT_AMALGAMATION_SH
#define RAFT_AMALGAMATION_SH
'

cat include/raft.h
cat include/raft_*.h
cat src/raft*.c | sed 's/#include "raft.*.h"//g' | sed 's/__/__raft__/g'

echo '#endif /* RAFT_AMALGAMATIONE_SH */'
