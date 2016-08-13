# Elastic Search benchmark for Search Analysis
_G. LEGRAND_

This package was built to benchmark Elastic Search in a Business Intelligence context (I.e. mostly data aggregation). It allows loading data in an efficient way and to run some aggregation queries (and record the response times). Even though we used a Travel Search data sample (see data/ directory) the code is quite generic and could be re-used for another data source. 

## Elastic Search installation and tuning

To install Elastic Search just follow the [official QuickStart](https://www.elastic.co/guide/en/elasticsearch/reference/current/_installation.html)

After installing Elastic Search there are a few settings that can be changed to improve performances. 
First the memory allocated to Elastic search (ES_HEAP_SIZE) should be set to 31GB (default is 1GB and performances become worse above 31GB because the JVM doesn't manage pointers efficiently after 32GB).

When benchmarking Elastic Search the main problem we faced was the insert throughput. Most of our tuning was thus to increase it. Especially some settings in elasticsearch.yml are worth changing:

* bootstrap.mlockall: true               --> to avoid swapping
* indices.memory.index_buffer_size: 50%  --> to allocate 50% of the memory to data load (default 10%)
* threadpool.bulk.queue_size: 1000       --> increase the insert queue size (should be higher than the number of insert jobs x number impacted shard per bulk)

## Content of the package

The package offers the following utilities:

* el_config.py:       configuration, most specificities about the data source
* el_drop.py:         drops the Elastic Search index
* el_create_index.py: create the Elastic Search index (especially defines its mapping)
* el_delete_file.py:  delete the content of a file into the Elastic Search index
* el_load.py:         load the content of a file into the Elastic Search index
* el_load_all.py:     load the content of a directory into the Elastic Search index
* el_data_reader.py:  decodes a line of the input data source (here travel search)
* el_search_bench.py: runs a set of search queries and record their response times (the queries are specific to the Travel Search data source)

We directly used the Elastic Search REST APIs. We could have used the python client elasticsearch-py. The code would have been a bit cleaner...

The typical benchmark scenario:

```
./el_create_index.py
./el_load_all.py ../data > load_stat.out
./el_search_bench.py     > search_stat.out
```

### Data load

The data load script el_load_all.py allows loading all files from a directory. If it is stopped it knows how to restart: it maintains a list of already processed files, delete the content of the ongoing data file (when it was stopped) from the data store and restart there.

To increase the throughput it is better to launch several loader scripts in parallel (taking their data from different directries). 

The load process inserts bulks of data for better efficiency.

Finally the load process logs (on stdout) the size and response time for each inserted bulk. Not sure it is reliable though since the load can be done asynchronously by the data store. It is probably better to look at the time necessary to load a big set of data.


## Some tips

### Data mapping

If you don't need to access the original document you can specify to not store it in the mapping (_source: {enabled: False} ). It will save disk space
If you don' t want to make search over all fields (e.g. give me the document where one field matches with "PAR") then disable the _all
Change the default indexing scheme of your string fields with index: not_analyzed. This will prevent a more expensive indexing that allow search on a part of the string
You can choose not to index some fields if you won't do any search on these criteria (but I guess if you also disable _source then there is no way to retrieve them...)

With Elastic Search it is not straightforward to group by multiple fields, for example top 10 Origin/Destination. The work arround is - within a single query - to group by Origin bucket and then to get the top destinations for each bucket (nested agg). It seemed worth to add a specific OnD field to make it more efficient. We observed 45 to 97% better response times.

### Indices and Sharding

Multiple indices: a good practice is to create new index for every period of time, typically every month like SA_2015_01, SA_2015_02. It thus gives flexibility to increase the number of shards as volume/cluster grows. Most of queries have a Search range time which can take advantage of the multiple indices. Nothing prevents to query multiple indices at once: it is transparent for the application. Here we only created one index as the data sample is quite small.

Each index can be sharded. The number of shards has to be defined at the creation of the index. We got optimal insert throughput for 5 to 10 shards (just for one server). Too many shards is not good for performances.

To improve the throughput it also possible to disable the replication during the insert and to activate it later. It is much more efficient.  

#### Routing

We used the OnD to route data accross shards. This insure you won't find data for a given OnD in 2 different shards. You can specify in the mapping whether you want to control the routing key (which means you'll have to specify a routing key when inserting data).

If you want to retrieve some records it is better if they are located in a single shard (if you can specify the routing key in the query). Now for aggregations of big amount of data we observed significantly better performances when the data are distributed over several shards (I guess the search is parallelized). In some cases you can help the aggregation query: for example if your routing key is OnD and you are looking at the top 10 OnDs you can tell Elastic Search to only get the top 10 per shard (that's enough to ensure we have the top 10 overall) with shard_size setting in the query.

### Data load tuning

Of course bulk insert should be used. The optimal size of the bulks is a matter of trial and errors (it is set in el_config.py). Besides running several load jobs in parallel increases the throughput.
Refresh interval should be modified, typically from 1s (default) to a few minutes. A refresh is basically the integration of pending updates in the indices. You can run a forcemerge command when you want your pending updates to be available.

Finally we noticed that having homgeneous bulks almost doubles the insert throughput (i.e. one bulk only targets one shard - typically if all data of the bulk have the same routing key).

### Marvel

Marvel is a wonderful plug-in that allows monitoring activity on the Elastic Search instance, especially insert throughput and DB size.