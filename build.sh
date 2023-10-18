#!/bin/bash
cd `dirname $0`
set -ex

g++ main.cc -std=c++17 -O2 -I./json/include -lcurl
