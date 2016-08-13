
##########################
### MongoDB Configuration
##########################


### MongoDB access

host="localhost"
port=27017



### Collection description

db="search"
collection="sample"
collection="global"



### Data load

bulk_buffer_size_max=100000
track_file="mg_mongo_track.txt"

# Data decoding function
import mg_data_reader
decode_input_line=mg_data_reader.decode_search_line


### List of indexes to be created ###
import pymongo
indexes=[
    [("OriginCity",pymongo.ASCENDING), ("DestinationCity",pymongo.ASCENDING)],
    [("OriginCity",pymongo.ASCENDING), ("Sampling",pymongo.ASCENDING)],
    [("SearchMonth",pymongo.ASCENDING),("Sampling",pymongo.ASCENDING)],
    [("DepartureMonth",pymongo.ASCENDING),("Sampling",pymongo.ASCENDING)],
    [("Sampling",pymongo.ASCENDING)],
    [("dataFile",pymongo.ASCENDING)]
]



### Benchmark

# the benchmark queries and logic are directly in the mg_search_bench.py file. Making that code generic as well would make it unreadable.
