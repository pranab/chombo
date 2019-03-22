## Introduction
Hadoop based ETL and various utility classes for Hadoop and Storm

## Philosophy
* Simple to use
* Input output in CSV format
* Metadata defined in simple JSON file
* Extremely configurable with many configuration knobs

## Solution
* Various relational algebra operation, including Projection, Join etc
* Data extraction ETL to extract structured record from unstructured data
* Data extraction ETL to extract structured record from JSON data
* Data validation ETL with configurable rules and statistical parameters 
* Data profiling ETL with various techniques
* Data transformation ETL with configurable transformation rules
* Various statistical data exploration solutions
* Data normalization
* Seasonal data analysis
* Various statistical parameter  calculation 
* Various long term statistical parameter calculation with incremental data  
* Bulk inset, update and delete of Hadoop data
* Bases classes for Storm Spout and Bolt
* Utility classes for string, configuration
* Utility classes for Storm and Redis

## Blogs
The following blogs of mine are good source of details. These are the only source
of detail documentation. Map reduce jobs in this projec are used in other projects
including sifarish, avenir etc. Blogs related to thos projects are also relevant.

* http://pkghosh.wordpress.com/2015/04/26/bulk-insert-update-and-delete-in-hadoop-data-lake/
* https://pkghosh.wordpress.com/2015/06/08/data-quality-control-with-outlier-detection/
* https://pkghosh.wordpress.com/2015/07/28/validating-big-data/
* https://pkghosh.wordpress.com/2015/09/22/profiling-big-data/
* https://pkghosh.wordpress.com/2015/08/25/anomaly-detection-with-robust-zscore/
* https://pkghosh.wordpress.com/2015/10/22/operational-analytics-with-seasonal-data/
* https://pkghosh.wordpress.com/2015/11/17/transforming-big-data/
* https://pkghosh.wordpress.com/2016/10/04/simple-sanity-checks-for-data-correctness-with-spark/
* https://pkghosh.wordpress.com/2016/12/06/json-to-relational-mapping-with-spark/
* https://pkghosh.wordpress.com/2017/02/19/mobile-phone-usage-data-analytics-for-effective-marketing-campaign/
* https://pkghosh.wordpress.com/2017/05/30/mining-seasonal-products-from-sales-data/
* https://pkghosh.wordpress.com/2017/07/26/processing-missing-values-with-hadoop/
* https://pkghosh.wordpress.com/2017/08/15/measuring-campaign-effectiveness-for-an-online-service-on-spark/
* https://pkghosh.wordpress.com/2017/11/07/removing-duplicates-from-order-data-using-spark/
* https://pkghosh.wordpress.com/2017/12/05/data-normalization-with-spark/
* https://pkghosh.wordpress.com/2018/01/10/data-type-auto-discovery-with-spark/
* https://pkghosh.wordpress.com/2018/08/20/pluggable-rule-driven-data-validation-with-spark/
* https://pkghosh.wordpress.com/2018/11/19/bulk-mutation-in-an-integration-data-lake-with-spark/
* https://pkghosh.wordpress.com/2019/03/21/plugin-framework-based-data-transformation-on-spark/


## Build
For Hadoop 1
* mvn clean install

For Hadoop 2 (non yarn)
* git checkout nuovo
* mvn clean install

For Hadoop 2 (yarn)
* git checkout nuovo
* mvn clean install -P yarn

For spark
* Build chombo first in master branch with 
	* mvn clean install  
	* sbt publishLocal
* Build chombo-spark in  chombo/spark directory
	* sbt clean package

## Need help?
Please feel free to email me at pkghosh99@gmail.com

## Contribution
Contributors are welcome. Please email me at pkghosh99@gmail.com

