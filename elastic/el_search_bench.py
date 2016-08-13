#!/usr/bin/env python

########################################################################
### Elastic Search benchmark with various set of aggregation queries ###
########################################################################
# This code is quite specific to our data (travel searches)

import json
import httplib
import datetime
import sys

##########################
## Queries definitions ###
##########################

########################################################################################
# top 10 destinations (with nb of searches) for a given market, origin and search month
def get_top_dest_for_market_and_origin_and_smonth(mkt, origin, search_month, sampling):

    topN=10

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "OriginCity":origin}},
                            {"term": { "SearchMonth":search_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_destination": {
                "terms" : {
                    "field" : "DestinationCity",
                    "shard_size": topN,
                    "size": topN
                },
                "aggs": {
                    "sum_destination": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)

    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations.per_destination.buckets.key,aggregations.per_destination.buckets.sum_destination"

    ### ROUTING KEY IS DEFINED HERE ###
    routing = "" # routing is unknown as we do not target a specific OnD

    return (query_json , routing, filter_path)
########################################################################################




########################################################################################
# top 10 OnD (with nb of searches) for a given market and search month
def get_top_ond_for_market_and_smonth(mkt, search_month, sampling):

    """ # Old query with a multiple field group by  (Origin-destination) replaced by a monofield group by (OnD)
    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "SearchMonth":search_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_destination": {
                "terms" : {
                    "field" : "DestinationCity",
                    "size": topN
                },
                "aggs": {
                    "sum_destination": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                },
                "aggs": {
                    "per_origin" : {
                        "terms" : {
                            "field" : "OriginCity",
                            "order" : { "sum_ond" : "desc" },
                            "shard_size": topN,
                            "size": topN
                        },
                        "aggs": {
                            "sum_ond": {
                                "sum": {
                                    "field": "Count"
                                }
                            }
                        }
                    }
                }  
            }
        }
    }

    filter_path="took,timed_out,_shards,hits,aggregations.per_destination.buckets.key,aggregations.per_destination.buckets.sum_origin,aggregations.per_destination.buckets.per_origin.buckets.key,aggregations.per_destination.buckets.per_origin.buckets.sum_ond"
"""

    topN=10

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "SearchMonth":search_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_ond": {
                "terms" : {
                    "field" : "OnD",
                    "shard_size": topN,
                    "size": topN
                },
                "aggs": {
                    "sum_ond": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)

    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations.per_ond.buckets.key,aggregations.per_ond.buckets.sum_ond"
    
    ### ROUTING KEY IS DEFINED HERE ###
    routing = "" # routing is unknown as we do not target a specific OnD

    return (query_json , routing, filter_path)
########################################################################################




########################################################################################
# top 10 OnD (with nb of searches) for a given market and departure month
def get_top_ond_for_market_and_dmonth(mkt, dep_month, sampling):

    topN=10

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "DepartureMonth":dep_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_destination": {
                "terms" : {
                    "field" : "DestinationCity",
                    "size": topN
                },
                "aggs": {
                    "sum_destination": {
                        "sum": {
                            "field": "Count"
                        }
                }
                },
                "aggs": {
                    "per_origin" : {
                        "terms" : {
                            "field" : "OriginCity",
                            "order" : { "sum_ond" : "desc" },
                            "shard_size": topN,
                            "size": topN
                        },
                        "aggs": {
                            "sum_ond": {
                                "sum": {
                                    "field": "Count"
                                }
                            }
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)

    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations.per_destination.buckets.key,aggregations.per_destination.buckets.sum_origin,aggregations.per_destination.buckets.per_origin.buckets.key,aggregations.per_destination.buckets.per_origin.buckets.sum_ond"
    
    ### ROUTING KEY IS DEFINED HERE ###
    routing = "" # routing is unknown as we do not target a specific OnD

    return (query_json , routing, filter_path)
########################################################################################




########################################################################################
# nb of searches per departure date for a given market and search month
def get_per_dep_date_for_market_and_smonth(mkt, search_month, sampling):

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "SearchMonth":search_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_depdate": {
                "terms" : {
                    "field" : "DepartureDate",
                    "size": 0
                },
                "aggs": {
                    "sum_dep": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)

    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations.per_depdate.buckets.key_as_string,aggregations.per_depdate.buckets.sum_dep"

    ### ROUTING KEY IS DEFINED HERE ###
    routing = "" # routing is unknown as we do not target a specific OnD

    return (query_json , routing, filter_path)
########################################################################################




########################################################################################
# nb of searches per stay duration for a given market and search month
def get_per_stay_for_market_and_ond_and_smonth(mkt, origin, destination, search_month, sampling):

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "OriginCity":origin}},
                            {"term": { "DestinationCity":destination}},
                            {"term": { "SearchMonth":search_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_depdow": {
                "terms" : {
                    "field" : "StayDuration",
                    "size" :  0
                },
                "aggs": {
                    "sum_dow": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)
    
    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations"

    ### ROUTING KEY IS DEFINED HERE ###
    routing = origin+destination # routing is knonw here!

    return (query_json, routing, filter_path)
########################################################################################




########################################################################################
# nb of searches per search date for a given market and departure month
def get_per_search_date_for_market_and_dmonth(mkt, dep_month, sampling):

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "DepartureMonth":dep_month}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_searchdate": {
                "terms" : {
                    "field" : "SearchDate",
                    "size": 0
                },
                "aggs": {
                    "sum_search": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)

    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations.per_searchdate.buckets.key_as_string,aggregations.per_searchdate.buckets.sum_search"

    ### ROUTING KEY IS DEFINED HERE ###
    routing = "" # routing is unknown as we do not target a specific OnD

    return (query_json , routing, filter_path)
########################################################################################




########################################################################################
# nb of searches per day of the week for a given market, origin country and destinatin country
def get_per_depdow_for_market_and_ctries(mkt, origin_ctry, destination_ctry, sampling):

    query={
        "size" : 0,
        "query": { 
            "filtered": {
                "filter": {
                    "bool" : {
                        "must": [
                            {"term": { "Market":mkt}},
                            {"term": { "OriginCountry":"%s"}},
                            {"term": { "DestinationCountry":"%s"}},
                            {"range" : {"Sampling" : {"lt" : sampling}}}
                        ]
                    }
                }
            }
        },
        "aggs" : {
            "per_depdow": {
                "terms" : {
                    "field" : "DepartureWeekDay",
                    "size": 0
                },
                "aggs": {
                    "sum_search": {
                        "sum": {
                            "field": "Count"
                        }
                    }
                }  
            }
        }
    }
    query_json=json.dumps(query)
    
    # Only this "JSON paths" need to be returned: that lightens the output
    filter_path="took,timed_out,_shards,hits,aggregations.per_depdow.buckets.key,aggregations.per_depdow.buckets.sum_search"

    ### ROUTING KEY IS DEFINED HERE ###
    routing = "" # routing is unknown as we do not target a specific OnD

    return (query_json ,routing , filter_path)
########################################################################################





##################
### Run Search ###
##################

def run_search(cnx,config, search_name,query_json, routing, filter_path):
    """ runs a search and records some stats on stdout

    Stats are on the following format:
      SEARCH: <name of the search>, <routing>, <nb of docs that match>, <search time recorded by Elastic>, <search time>

    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings (including the name of the index)
      search_name: name of the search (to identify it in the recorded stats)
      query_json:  query to be ran
      routing:     routing key (empty string if none)
      filter_path: "JSON paths" to be returned by elastic search (that lightens the output)

    """

    # Sending Search request to Elastic Search and printing the results
    sys.stderr.write( "Running search %s\n" % search_name)
    time1=datetime.datetime.now()

    # The routing is specified in the URL
    routing_str=""
    if routing != "" : # if not empty string then we specify a routing
        routing_str="&routing="+routing

    # To limit the returned data we specify in the URL what fields of the standard output we want to keep
    ##########################################################################################################################
    cnx.request("POST",config.index+"/"+config.typ+"/_search?pretty"+routing_str+"&filter_path="+filter_path,query_json)
    ##########################################################################################################################
    resp=cnx.getresponse()
    reply_json=resp.read()
    time2=datetime.datetime.now()
    #sys.stderr.write( reply_json + "\n")
    reply=json.loads(reply_json)
    if resp.status != httplib.OK:
        raise Exception("ERROR when running" + search_name)
    
    # The following print records KPIs to be gathered to build the benchmark results
    ##########################################################################################################################
    print "SEARCH: %s,%s,%d,%d,%d" % (search_name, routing,reply["hits"]["total"], reply["took"], int((time2-time1).total_seconds()*1000) )
    ##########################################################################################################################
   

############
### TEST ###
############

def test(cnx, config, sampling=100):
    """ Reduced benchmark secnario set
    
    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings (including the name of the index)
      sampling: sampling ratio to be applied (between 1 and 100)

    """

    sys.stderr.write("Starting minimal benchmark scenarios for testing purpose\n")

    run_search(cnx,config,"TopDestForMktOriSMonth-%d"%sampling,*get_top_dest_for_market_and_origin_and_smonth("FR","PAR","2015-03",sampling))
    run_search(cnx,config,"TopOnDForMktSMonth-%d"%sampling,*get_top_ond_for_market_and_smonth("FR","2015-03",sampling))
    run_search(cnx,config,"TopOnDForMktDMonth-%d"%sampling,*get_top_ond_for_market_and_dmonth("FR","2015-11",sampling))
    run_search(cnx,config,"PerDDateForMktSMonth-%d"%sampling,*get_per_dep_date_for_market_and_smonth("FR","2015-04",sampling))
    run_search(cnx,config,"PerSDateForMktDMonth-%d"%sampling,*get_per_search_date_for_market_and_dmonth("FR","2015-12",sampling))
    run_search(cnx,config,"PerStayForMktOnDSMonth-%d"%sampling,*get_per_stay_for_market_and_ond_and_smonth("FR","PAR","NYC","2015-03",sampling))
    run_search(cnx,config,"PerDDOWForMktCtries-%d"%sampling,*get_per_depdow_for_market_and_ctries("FR","FR","US",sampling))

    sys.stderr.write("Minimal benchmark scenarios finished\n")
    


#############
### BENCH ###
#############

def bench(cnx, config):
    """Benchmark based on various aggregation queries.

    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings (including the name of the index)

    """

    ### creating some input values ###

    samplings=[1000,200,40,8]
    #samplings=[1000]
    mkts=["FR"]
    origins=["PAR","NCE","LYS"]
    destinations=["NYC","MAD"]
    origin_ctries=["FR","BE"]
    dest_ctries=["FR","US","GR"]
    dep_months=["2015-10","2015-11","2015-12"]
    search_months=["2015-02","2015-03","2015-04"]
    

    date_format = "%Y-%m-%d"
    import itertools
    import random

    ##########################################
    # Running the different search scenarios #
    
    sys.stderr.write("Starting benchmark scenarios\n")

    # Depending on the query we don't have the same set of inputs. Hence the different generations of inputs
    
    # Creating all the possible input combinations: market, search month, sampling
    test_cases = list(itertools.product(mkts,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,search_month, sampling) in test_cases:
        sys.stderr.write( str((mkt,search_month, sampling))+"\n")
        run_search(cnx,config,"TopOnDForMktSMonth-%d"%sampling,*get_top_ond_for_market_and_smonth(mkt,search_month, sampling))
        run_search(cnx,config,"PerDDateForMktSMonth-%d"%sampling,*get_per_dep_date_for_market_and_smonth(mkt,search_month,sampling))

    # Creating all the possible input combinations: market, dep month, sampling
    test_cases = list(itertools.product(mkts,dep_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,dep_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,dep_month, sampling))+"\n")
        run_search(cnx,config,"TopOnDForMktDMonth-%d"%sampling,*get_top_ond_for_market_and_dmonth(mkt,dep_month,sampling))
        run_search(cnx,config,"PerSDateForMktDMonth-%d"%sampling,*get_per_search_date_for_market_and_dmonth(mkt,dep_month,sampling))

    # Creating all the possible input combinations: market, origin, search_month, sampling
    test_cases = list(itertools.product(mkts,origins,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin,search_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin,search_month,sampling))+"\n")
        run_search(cnx,config,"TopDestForMktOriSMonth-%d"%sampling,*get_top_dest_for_market_and_origin_and_smonth(mkt,origin, search_month,sampling))

    # Creating all the possible input combinations: market, origin, destination, search_month, sampling
    test_cases = list(itertools.product(mkts,origins,destinations,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin,destination,search_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin,destination,search_month,sampling))+"\n")
        run_search(cnx,config,"PerStayForMktOnDDMonth-%d"%sampling,*get_per_stay_for_market_and_ond_and_smonth(mkt,origin,destination, search_month,sampling))

    # Creating all the possible input combinations: market, origin country, destination country, sampling
    test_cases = list(itertools.product(mkts,origin_ctries,dest_ctries, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin_ctry,destination_ctry,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin_ctry,destination_ctry,sampling))+"\n")
        run_search(cnx,config,"PerDDOWForMktCtries-%d"%sampling,*get_per_depdow_for_market_and_ctries(mkt ,origin_ctry,destination_ctry,sampling))

    sys.stderr.write("Benchmark scenarios finished\n")
    


############
### MAIN ###
############

if __name__ == '__main__':

    # All our configuration variables
    import el_config
    
    # Connection to Elastic Search
    sys.stderr.write( "Connecting to %s on port %d\n" % (el_config.host,el_config.port))
    cnx=httplib.HTTPConnection(el_config.host,el_config.port)
    cnx.connect()
    
    import argparse
    
    # Parsing arguments
    parser = argparse.ArgumentParser(description='Benchmark Elastic Search with various aggregation queries.')
    parser.add_argument('-t', '--test', dest='test', action='store_true', help='Reduced scenario for testing')
    args = parser.parse_args()
    
    if args.test:
        #####################
        test(cnx,el_config) #
        #####################
    else: 
        ######################
        bench(cnx,el_config) #
        ######################
        
        
    cnx.close()


