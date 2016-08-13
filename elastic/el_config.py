
#################################
### Elastic Search Configuration
#################################


### Elastic Search access

#host="nceorihad56.nce.amadeus.net"    # Elastic Search server
host="localhost"                     # Elastic Search server
port=9200                             # Elastic Search port



### Index description

index="search"                # Index name
index="test"                          # Index name
typ="counters"                        # Type name
shard_nb=2                            # number of shards
replica_nb=0                          # replication factor



### Data load

track_file="el_track.txt"           # Where loaded file name are recorded (load restart point)
bulk_buffer_size_max=50000            # Bulk size for load and delete
load_refresh_interval=1000          # Elastic Search setting to prevent permanent and inefficient indexing of new inserts
nb_retry_max=10                     # Number of allowed retries when loading a directory

# Definition of the ROUTING KEY of a document to be loaded
def getRouting(input): return input["OnD"]

# Data decoding function
import el_data_reader
decode_input_line=el_data_reader.decode_search_line

### Data mapping

mapping={"_routing": { "required" : True },   # Routing key is required when inserting data
        "_all":      { "enabled" :  False },  # We won't need to search for a pattern on all fields at once
        "_source":   { "enabled" :  False },  # Not storing the original document saves a lot of disk space
	"properties" : {
          "dataFile" :             { "type" : "string", "index" : "not_analyzed" }, # not_analyzed because we'll only need exact match on the string
          "itemNb" :               { "type" : "integer" },
          "Market" :               { "type" : "string", "index" : "not_analyzed" },
          "MarketContinent" :      { "type" : "string", "index" : "not_analyzed" },
          "SearchDate" :           { "type" : "date", "format" : "yyyy-MM-dd" },
          "SearchWeekDay" :        { "type" : "string", "index" : "not_analyzed" },
          "SearchWeek" :           { "type" : "string", "index" : "not_analyzed" },
          "SearchMonth" :          { "type" : "string", "index" : "not_analyzed" },
          "SearchQuarter" :        { "type" : "string", "index" : "not_analyzed" },
          "OriginCity" :           { "type" : "string", "index" : "not_analyzed" },
          "OriginCountry" :        { "type" : "string", "index" : "not_analyzed" },
          "OriginContinent" :      { "type" : "string", "index" : "not_analyzed" },
          "DestinationCity" :      { "type" : "string", "index" : "not_analyzed" },
          "DestinationCountry" :   { "type" : "string", "index" : "not_analyzed" },
          "DestinationContinent" : { "type" : "string", "index" : "not_analyzed" },
          "OnD" :                  { "type" : "string", "index" : "not_analyzed" },
          "DepartureDate" :        { "type" : "date", "format": "yyyy-MM-dd" },
          "DepartureWeekDay" :     { "type" : "string", "index" : "not_analyzed" },
          "DepartureWeek" :        { "type" : "string", "index" : "not_analyzed" },
          "DepartureMonth" :       { "type" : "string", "index" : "not_analyzed" },
          "DepartureQuarter" :     { "type" : "string", "index" : "not_analyzed" },
          "ReturnDate" :           { "type" : "date", "format": "yyyy-MM-dd" },
          "ReturnWeekDay" :        { "type" : "string", "index" : "not_analyzed" },
          "ReturnWeek" :           { "type" : "string", "index" : "not_analyzed" },
          "ReturnMonth" :          { "type" : "string", "index" : "not_analyzed" },
          "ReturnQuarter" :        { "type" : "string", "index" : "not_analyzed" },
          "AdvancePurchase" :      { "type" : "short" },
          "StayDuration" :         { "type" : "short" },
          "IsOneWay" :             { "type" : "boolean" },
          "Distance" :             { "type" : "float" },
          "Geography" :            { "type" : "string", "index" : "not_analyzed" },
          "Count" :                { "type" : "integer" },
          "Sampling" :             { "type" : "short" }
        }
}


### Benchmark

# the benchmark queries and logic are directly in the el_search_bench.py file. Making that code generic as well would make it unreadable.
