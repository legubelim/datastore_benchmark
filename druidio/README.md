# Druid IO benchmark
_G. LEGRAND_

This package was built to benchmark Druid IO in a Business Intelligence context (I.e. mostly data aggregation). It allows loading data in an efficient way and to run some aggregation queries (and record the response times). 

## Install and start

To install Druid IO just follow the [official QuickStart](http://druid.io/docs/0.9.1.1/tutorials/quickstart.html)

As you can see in the official documentation Druid IO requires to start ZooKeeper as well as 5 other java jobs. There are a few settings that you may want to change:

* By default the data will be stored in the install directory. 
* Change Zookeeper config (zoo.cfg) to not have dataDir targetting /tmp
* When running with local hadoop we found no other way to prevent writes into /tmp than to create symbolic links:

```
 ln -s /my/tmp/path/hadoop-legrandg /tmp/
 ln -s /my/tmp/path/hsperfdata_legrandg /tmp/
```

* We faced failures because of a limit in the number of allowed thread in the configuration: we had to increase the setting druid.server.http.numThreads from 8 to 10 in some runtime.properties file

The script ./start_druid.sh launches the commands to start the server jobs. So far we did not find the graceful way to stop them (we had to use kill -9 !!) but no doubt there must be one.

## Content of the package

The package offers the following utilities:

* dr_config.py:       configuration, most specificities about the data source
* dr_drop.py:         drops the Elastic Search index
* dr_create_index.py: create the Elastic Search index (especially defines its mapping)
* dr_delete_file.py:  delete the content of a file into the Elastic Search index
* dr_load.py:         load the content of a file into the Elastic Search index
* dr_load_all.py:     load the content of a directory into the Elastic Search index
* dr_data_reader.py:  decodes a line of the input data source (here travel search)
* dr_search_bench.py: runs a set of search queries and record their response times (the queries are specific to the Travel Search data source)

We directly used the Druid IO REST APIs. We could have used a python client like pydruid. The code would have been a bit cleaner...

The typical benchmark scenario:

```

./dr_load_all.py /absolute/path/to/data/
./dr_search_bench.py     > search_stat.out

```

## Data load

We used the batch ingestion feature to load the file. The dr_load_all.py script basically submit a druid task that will read and load the file(s). When submitting the task you specify the layout of the file(s). It means you cannot add a data pre-processing step.

## Druid IO Console

The druid console is very useful: http://nceorihad56:8081/#/
It shows the segments of each data source and the status of the different tasks. It also allows to remove ("disable") a data source 