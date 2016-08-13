# MongoDB benchmark for Search Analysis
_G. LEGRAND_

This package was built to benchmark MongoDB in a Business Intelligence context (I.e. mostly data aggregation). It allows loading data in an efficient way and to run some aggregation queries (and record the response times). Even though we used a Travel Search data sample (see data/ directory) the code is quite generic and could be re-used for another data source. 

## MongoDB installation and tuning

To install Elastic Search just follow the [official QuickStart](https://docs.mongodb.com/getting-started/shell/tutorial/install-mongodb-on-linux/)

You also need the MongoDB Python client pymongo to run the benchmark

## Content of the package

The package offers the following utilities:

* mg_config.py:         configuration, most specificities about the data source
* mg_drop.py:           drops the MongoDB collection
* mg_create_indexes.py: create the Mongo DB indexes
* mg_load.py:           load the content of a file into the MongoDB collection
* mg_load_all.py:       load the content of a directory into the MongoDB collection - knows where to restart in case of failure
* mg_data_reader.py:    decodes a line of the input data source (here travel search)
* mg_search_bench.py:   runs a set of search queries and record their response times (the queries are specific to the Travel Search data source)

We used the MongoDB Python client pymongo.

The typical benchmark scenario:

```

./mg_drop.py
./mg_create_indexes.py
./mg_load_all.py ../data > load_stat.out
./mg_search_bench.py     > search_stat.out

```

### Data load

The data load script mg_load_all.py allows loading all files from a directory. If it is stopped it knows how to restart: it maintains a list of already processed files, delete the content of the ongoing data file (when it was stopped) from the data store and restart there.

To increase the throughput it is better to launch several loader scripts in parallel (taking their data from different directries). 

The load process inserts bulks of data for better efficiency.

Finally the load process logs (on stdout) the size and response time for each inserted bulk. Not sure it is reliable though since the load can be done asynchronously by the data store. It is probably better to look at the time necessary to load a big set of data.


## Tips 

### sharding and multiple collections

In this benchmark we did not use the sharding feature. That's significantly more complex to set up compared to a mono instance DB. Of course that becomes mandatory as soon as you want a MongoDB distributed over a cluster of machines.

Another way to partition the data is to define several collections although multiple collections means additional dev as you may have to run several queries in parallel (for each impacted collection) and to merge the results.

### Indexes

In MongoDB you have to define your indexes to speed up your queries. That's tricky. For example (in our Travel Search context) we can define a first index on Market/SearchMonth/OnD to speed up the queries looking for the top OnDs for a given Market and SearchMonth. However it won't be fit for a query looking for the top OnDs for a given Market and DepartureMonth. Besides you cannot define indexes for every search use cases if you have too many: that would be a too big overhead during the insert.

Another issue is the sampling column (see data description in ../data): where to place it in the index? As a first column it will be suited in case of strong sampling (e.g. 1%) but it will completely spoil your index you are looking at 100% of the data.

### Aggregation and Pipelines

From what we understood of the execution plans mongoDB does not much take advantage from the indexes when running aggregations. When looking for the top OnDs of a market and Search Month it looks like MongoDB only uses the Market and search Month part of the index whereas it could use the OnD part to ease the grouping. We have the feeling it is linked to the pipeline design of the query engine: First step is to retrieve the data matching the filters, 2nd step is to group the retrieved data. At the end f the first step the index information is lost... On the other hand the pipeline queries are very easy to write.

### Data load

It is important to use bulk load to have good performances. The optimal size of the bulks is a matter of trial and errors (it is set in mg_config.py). Besides running several load jobs in parallel increases the throughput.
Besides we noticed that reducing the size of the attribute names in the inserted document significantly reduces the required disk space.

