## Introduction
Set of helper classes for Hadoop and Storm, Hadoop based ETL

## Philosophy
* Simple to use
* Input output in CSV format
* Metadata defined in simple JSON file
* Extremely configurable with many configuration knobs

## Solution
* Map reduce classes for various relational algebra operation, including Projection, Join etc
* Various ETL map reduce jobs
* Data validation ETL
* Exploratory statistics map reduce
* Bulk mutation of HDFS data
* Bases classes for Storm Spout and Bolt
* Utility classes for string, configuration
* Utility classes for Redis

## Blogs
The following blogs of mine are good source of details. These are the only source
of detail documentation. Map reduce jobs in this projec are used in other projects
including sifarish, avenir etc. Blogs related to thos projects are also relevant.
* http://pkghosh.wordpress.com/2015/04/26/bulk-insert-update-and-delete-in-hadoop-data-lake/

## Build
For Hadoop 1
* mvn clean install

For Hadoop 2 (non yarn)
* git checkout nuovo
* mvn clean install

For Hadoop 2 (yarn)
* git checkout nuovo
* mvn clean install -P yarn

## Help
Please feel free to email me at pkghosh99@gmail.com

## Contribution
Contributors are welcome. Please email me at pkghosh99@gmail.com

