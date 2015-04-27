## Introduction
Set of helper classes for Hadoop and Storm

## Solution
* Map reduce classes for various relational algebra operation, including Projection, Join etc
* Various ETM map reduce
* Exploratory statistics map reduce
* Bulk mutation of HDFS data
* Bases classes for Storm Spout and Bolt
* Utility classes for string, configuration
* Utility classes for Redis

## Build
For Hadoop 1
mvn clean install

For Hadoop 2 (non yarn)
git checkout nuovo
mvn clean install

For Hadoop 2 (yarn)
git checkout nuovo
mvn clean install -P yarn

## Help
Please feel free to email me at pkghosh99@gmail.com
