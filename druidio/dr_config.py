

###########################
### Druid IO Configuration
###########################


### Drui IO access

host="localhost"                   # Druid IO server
query_port=8083                    # port for queries
query_url="/druid/v2"              # URL for queries
load_port=8090                     # port for submitting ingestion tasks
load_url="/druid/indexer/v1/task"  # URL for submitting ingestion tasks



### Data source

data_source="search_global"        # Data source name



### Data load

def getIngest(paths, data_source_name, search_date1="2015-01-01", search_date2="2015-12-31"): 
    """ returns the object to submit an ingestion task in druid IO

    Parameters:
      paths :    paths of the files to be loaded
      data_source_name : name given to the data set in Druid IO
      Search_date1/2 : date defining the time interval of the data being loaded

    """
    return {
        "type" : "index_hadoop",
        "spec" : {
            "ioConfig" : {
                "type" : "hadoop",
                "inputSpec" : {
                    "type" : "static",
                    "paths" : paths
                }
            },
            "dataSchema" : {
                "dataSource" : data_source_name,
                "granularitySpec" : {
                    "type" : "uniform",
                    "segmentGranularity" : "day",
                    "queryGranularity" : "none",
                    "intervals" : [search_date1+"/"+search_date2]
                },
                "parser" : {
                    "type" : "string",
                    "parseSpec" : {
                        "format" : "tsv",
                        "columns" : [
                            "Market",
                            "MarketContinent",
                            "SearchDate",
                            "SearchWeekDay",
                            "SearchWeek",
                            "SearchMonth",
                            "SearchQuarter",
                            "OriginCity",
                            "OriginCountry",
                            "OriginContinent",
                            "DestinationCity",
                            "DestinationCountry",
                            "DestinationContinent",
                            "OnD",
                            "DepartureDate",
                            "DepartureWeekDay",
                            "DepartureWeek",
                            "DepartureMonth",
                            "DepartureQuarter",
                            "AdvancePurchase",
                            "Distance",
                            "Geography",
                            "Count",
                            "Sampling",
                            "IsOneWay",
                            "StayDuration",
                            "ReturnDate",
                            "ReturnWeekDay",
                            "ReturnWeek",
                            "ReturnMonth",
                            "ReturnQuarter"
                        ],
                        "delimiter":"^",
                        "dimensionsSpec" : {
                            "dimensions" : [
                                "Market",
                                "MarketContinent",
                                "SearchDate",
                                "SearchWeekDay",
                                "SearchWeek",
                                "SearchMonth",
                                "SearchQuarter",
                                "OriginCity",
                                "OriginCountry",
                                "OriginContinent",
                                "DestinationCity",
                                "DestinationCountry",
                                "DestinationContinent",
                                "OnD",
                                "DepartureDate",
                                "DepartureWeekDay",
                                "DepartureWeek",
                                "DepartureMonth",
                                "DepartureQuarter",
                                "AdvancePurchase",
                                "Distance",
                                "Geography",
                                "Count",
                                "Sampling",
                                "IsOneWay",
                                "StayDuration",
                                "ReturnDate",
                                "ReturnWeekDay",
                                "ReturnWeek",
                                "ReturnMonth",
                                "ReturnQuarter"
                            ]
                        },
                        "timestampSpec" : {
                            "format" : "auto",
                            "column" : "SearchDate"
                        }
                    }
                },
                "metricsSpec" : [
                    {
                        "name" : "cardinality",
                        "type" : "count"
                    },
                    {
                        "name" : "Count_met",
                        "type" : "doubleSum",
                        "fieldName" : "Count"
                    },
                    {
                        "name" : "PaxCount_met",
                        "type" : "doubleSum",
                        "fieldName" : "PaxCount"
                    }
                ]
            },
            "tuningConfig" : {
                "type" : "hadoop",
                "partitionsSpec" : {
                    "type" : "hashed",
                    "targetPartitionSize" : 5000000
                },
                "jobProperties" : {}
            }
        }
    }
    
