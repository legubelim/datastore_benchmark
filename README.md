# Data stores benchmark
_G. LEGRAND_

These packages aim at exploring different data stores in the frame of a Business Intelligence context. We are looking here for a technology giving enough flexibility to aggregate big amount of data accross every dimension. This repository contains the benchmarks of 3 data stores:

* MongoDB
* Elastic Search
* Druid IO 

## Repository content

For each data store utilities are provided to load data and to run some aggregation queries (and record the response times). We tried to keep consistent file naming between data stores and the queries wear the same names and give the same results from one data store to another. For example:

* $$_config.py:       configuration, typically name of the index/collection/data source
* $$_load_all.py:     load the content of the directory in argument
* $$_search_bench.py: runs a benchmark of different queries

## Configuration

The configuration files allow to specify the host/port of the data store as well as the name of the index/collection/data source. It also gives parameters like the bulk load buffer size. 
Finally we put in the configuration as much settings related to the data source as possible. E.g. routing key, data layout... Still the queries - that are specific to the data set - are in the $$_search_bench.py modules.

## Data

The proposed data set corresponds to a sample of travel searches for Market France and year 2015. It is only a sample. The real data set is significantly bigger.
It can be downloaded from [Dropbox](https://www.dropbox.com/sh/covosmyni6narcj/AAAOrfKUohXtxVve5tshJcYFa?dl=0)

Data are  already aggregated at Origin / Destination / Search date/ Departure date / Return date level. For each of these key you have the number of searches. 
In order to be queried easily data are decorated with many additional information (E.g. countries, stay duration...). 

The final layout is:

* Market: Country where the search was issued (always FR)
* MarketContinent:
* SearchDate: Date when the search was issued
* SearchWeekDay:
* SearchWeek:
* SearchMonth:
* SearchQuarter:
* OriginCity: Origin city of the requested trip
* OriginCountry:
* OriginContinent:
* DestinationCity: Destination city of the requested trip
* DestinationCountry:
* DestinationContinent:
* OnD: Origin and destination concatenated
* DepartureDate:
* DepartureWeekDay:
* DepartureWeek:
* DepartureMonth:
* DepartureQuarter:
* AdvancePurchase: Number of days between search date and departure date
* Distance: Distance between Origin and Destination
* Geography: I (International) D (Domestic) R (Regional if Origin and Destination on same continent)
* Count: Number of travel searches
* Sampling: Random number between 0 and 99
* IsOneWay: True if One Way trip
* StayDuration: Number of days between departure date and return date
* ReturnDate:
* ReturnWeekDay:
* ReturnWeek:
* ReturnMonth:
* ReturnQuarter:


## Benchmark

### Queries

Different aggregation queries were defined that are representative of the use cases we could have for Search. It is not exhaustive though:

* get_top_dest_for_market_and_origin_and_smonth: top 10 destinations (with nb of searches) for a given market, origin and search month
* get_top_ond_for_market_and_smonth: top 10 OnD (with nb of searches) for a given market and search month
* get_top_ond_for_market_and_dmonth: top 10 OnD (with nb of searches) for a given market and departure month
* get_per_dep_date_for_market_and_smonth: nb of searches per departure date for a given market and search month
* get_per_search_date_for_market_and_dmonth: nb of searches per search date for a given market and departure month
* get_per_stay_for_market_and_ond_and_smonth: nb of searches per stay duration for a given market and search month
* get_per_depdow_for_market_and_ctries: nb of searches per day of the week for a given market, origin country and destinatin country

### Sampling

In many cases it is not necessarily worth to explore all data to get relevant results. A random sample is often enough to get very acurate results. In order to enable the application to explore less data the column "Sampling" has been added to the data layut. It is set with a random number between 0 and 99 at load time. It is thus possible at search time to specify a "Sampling<XX" clause to only explore a random sample of the data. Depending how specific the query is you may need different samping level to get sufficient accuracy. All in all this technic allows to make compromise between accuracy and response time.

### Inputs

In the benchmark script each query is ran several times with different sets of inputs: different search months, origins, departure months, origin countries, destination countries, sampling level.

### results

The benchmark script does not show the results of the queries but logs (in stdout) stats with the kind of search, the sampling level, the number of scanned data (if available) and the response time in order to allow comparison between data stores.