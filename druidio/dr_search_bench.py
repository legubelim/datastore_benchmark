#!/usr/bin/env python

##################################################################
### Druid IO benchmark with various set of aggregation queries ###
##################################################################
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
def get_top_dest_for_market_and_origin_and_smonth(data_source, mkt, origin, search_month, sampling):

    topN=10

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "DestinationCity",
        "threshold": topN,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "OriginCity",
                    "value": origin
                },
                {
                    "type": "selector",
                    "dimension": "SearchMonth",
                    "value": search_month
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }

    return json.dumps(query)
########################################################################################




########################################################################################
# top 10 OnD (with nb of searches) for a given market and search month
def get_top_ond_for_market_and_smonth(data_source, mkt, search_month, sampling):

    topN=10

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "OnD",
        "threshold": topN,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "SearchMonth",
                    "value": search_month
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }

    return json.dumps(query)
########################################################################################




########################################################################################
# top 10 OnD (with nb of searches) for a given market and departure month
def get_top_ond_for_market_and_dmonth(data_source, mkt, dep_month, sampling):

    topN=10

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "OnD",
        "threshold": topN,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "DepartureMonth",
                    "value": dep_month
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }
    
    return json.dumps(query)
########################################################################################




########################################################################################
# nb of searches per departure date for a given market and search month
def get_per_dep_date_for_market_and_smonth(data_source, mkt, search_month, sampling):

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "DepartureDate",
        "threshold": 400,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "SearchMonth",
                    "value": search_month
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }

    return json.dumps(query)
########################################################################################




########################################################################################
# nb of searches per stay duration for a given market and search month
def get_per_stay_for_market_and_ond_and_smonth(data_source, mkt, origin, destination, search_month, sampling):

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "StayDuration",
        "threshold": 400,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "OriginCity",
                    "value": origin
                },
                {
                    "type": "selector",
                    "dimension": "DestinationCity",
                    "value": destination
                },
                {
                    "type": "selector",
                    "dimension": "SearchMonth",
                    "value": search_month
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }

    return json.dumps(query)
########################################################################################




########################################################################################
# nb of searches per search date for a given market and departure month
def get_per_search_date_for_market_and_dmonth(data_source, mkt, dep_month, sampling):

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "SearchDate",
        "threshold": 400,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "DepartureMoonth",
                    "value": dep_month
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }

    return json.dumps(query)
########################################################################################




########################################################################################
# nb of searches per day of the week for a given market, origin country and destinatin country
def get_per_depdow_for_market_and_ctries(data_source, mkt, origin_ctry, destination_ctry, sampling):

    query={
        "queryType": "topN",
        "dataSource": data_source,
        "dimension": "DepartureWeekDay",
        "threshold": 10,
        "metric": "Count_met",
        "granularity": "all",
        "filter": {
            "type": "and",
            "fields": [
                {
                    "type": "selector",
                    "dimension": "Market",
                    "value": mkt
                },
                {
                    "type": "selector",
                    "dimension": "OriginCountry",
                    "value": origin_ctry
                },
                {
                    "type": "selector",
                    "dimension": "DestinationCountry",
                    "value": destination_ctry
                },
                {
                    "type": "bound",
                    "dimension": "Sampling",
                    "upper" : "%02d"%sampling,
                    "alphanumeric": True
                }
            ]
        },
        "aggregations": [
            {
                "type": "longSum",
                "name": "Count_met",
                "fieldName": "Count_met"
            }
        ],
        "intervals": [
            "2015-01-01T00:00:00.000/2016-01-01T00:00:00.000"
        ]
    }

    return json.dumps(query)
########################################################################################





##################
### Run Search ###
##################

headers = {'Content-type': 'application/json'}

def run_search(cnx,config, search_name,query_json):
    """ runs a search and records some stats on stdout

    Stats are on the following format:
      SEARCH: <name of the search>, <nb of returned docs>, <search time>

    Parameters:
      cnx:    HTTP connection to Druid IO
      config: Some settings (including the name of the data source)
      search_name: name of the search (to identify it in the recorded stats)
      query_json:  query to be ran

    """

    # Sending Search request to Elastic Search and printing the results
    sys.stderr.write( "Running search %s\n" % search_name)
    time1=datetime.datetime.now()

    # To limit the returned data we specify in the URL what fields of the standard output we want to keep
    ############################################################
    cnx.request("POST",dr_config.query_url,query_json,headers) #
    ############################################################
    resp=cnx.getresponse()
    reply_json=resp.read()
    time2=datetime.datetime.now()
    #sys.stderr.write( reply_json + "\n")
    reply=json.loads(reply_json)
    if resp.status != httplib.OK:
        raise Exception("ERROR when running" + search_name)
    
    # The following print records KPIs to be gathered to build the benchmark results
    ############################################################################################################
    print "SEARCH: %s,%d,%d" % (search_name,len(reply[0]["result"]), int((time2-time1).total_seconds()*1000) ) #
    ############################################################################################################
    #cnx.request("POST",dr_config.query_url+"/?pretty",query_json,headers)

   
############
### TEST ###
############

def test(cnx, config, sampling=100):
    """ Reduced benchmark secnario set
    
    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings (including the name of the data source)
      sampling: sampling ratio to be applied (between 1 and 100)

    """

    sys.stderr.write("Starting minimal benchmark scenarios for testing purpose\n")

    run_search(cnx,config,"TopDestForMktOriSMonth-%d"%sampling,get_top_dest_for_market_and_origin_and_smonth(config.data_source,"FR","PAR","2015-03",sampling))
    run_search(cnx,config,"TopOnDForMktSMonth-%d"%sampling,get_top_ond_for_market_and_smonth(config.data_source,"FR","2015-03",sampling))
    run_search(cnx,config,"TopOnDForMktDMonth-%d"%sampling,get_top_ond_for_market_and_dmonth(config.data_source,"FR","2015-11",sampling))
    run_search(cnx,config,"PerDDateForMktSMonth-%d"%sampling,get_per_dep_date_for_market_and_smonth(config.data_source,"FR","2015-04",sampling))
    run_search(cnx,config,"PerSDateForMktDMonth-%d"%sampling,get_per_search_date_for_market_and_dmonth(config.data_source,"FR","2015-12",sampling))
    run_search(cnx,config,"PerStayForMktOnDSMonth-%d"%sampling,get_per_stay_for_market_and_ond_and_smonth(config.data_source,"FR","PAR","NYC","2015-03",sampling))
    run_search(cnx,config,"PerDDOWForMktCtries-%d"%sampling,get_per_depdow_for_market_and_ctries(config.data_source,"FR","FR","US",sampling))

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

    samplings=[100,20,4]
    #samplings=[100]
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
        run_search(cnx,config,"TopOnDForMktSMonth-%d"%sampling,get_top_ond_for_market_and_smonth(config.data_source, mkt,search_month, sampling))
        run_search(cnx,config,"PerDDateForMktSMonth-%d"%sampling,get_per_dep_date_for_market_and_smonth(config.data_source, mkt,search_month,sampling))

    # Creating all the possible input combinations: market, dep month, sampling
    test_cases = list(itertools.product(mkts,dep_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,dep_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,dep_month, sampling))+"\n")
        run_search(cnx,config,"TopOnDForMktDMonth-%d"%sampling,get_top_ond_for_market_and_dmonth(config.data_source, mkt,dep_month,sampling))
        run_search(cnx,config,"PerSDateForMktDMonth-%d"%sampling,get_per_search_date_for_market_and_dmonth(config.data_source, mkt,dep_month,sampling))

    # Creating all the possible input combinations: market, origin, search_month, sampling
    test_cases = list(itertools.product(mkts,origins,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin,search_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin,search_month,sampling))+"\n")
        run_search(cnx,config,"TopDestForMktOriSMonth-%d"%sampling,get_top_dest_for_market_and_origin_and_smonth(config.data_source, mkt,origin, search_month,sampling))

    # Creating all the possible input combinations: market, origin, destination, search_month, sampling
    test_cases = list(itertools.product(mkts,origins,destinations,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin,destination,search_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin,destination,search_month,sampling))+"\n")
        run_search(cnx,config,"PerStayForMktOnDDMonth-%d"%sampling,get_per_stay_for_market_and_ond_and_smonth(config.data_source, mkt,origin,destination, search_month,sampling))

    # Creating all the possible input combinations: market, origin country, destination country, sampling
    test_cases = list(itertools.product(mkts,origin_ctries,dest_ctries, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin_ctry,destination_ctry,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin_ctry,destination_ctry,sampling))+"\n")
        run_search(cnx,config,"PerDDOWForMktCtries-%d"%sampling,get_per_depdow_for_market_and_ctries(config.data_source, mkt ,origin_ctry,destination_ctry,sampling))

    sys.stderr.write("Benchmark scenarios finished\n")
    


############
### MAIN ###
############

if __name__ == '__main__':

    # All our configuration variables
    import dr_config
    
    # Connection to Elastic Search
    sys.stderr.write( "Connecting to %s on port %d\n" % (dr_config.host,dr_config.query_port))
    cnx=httplib.HTTPConnection(dr_config.host,dr_config.query_port)
    cnx.connect()
    
    import argparse
    
    # Parsing arguments
    parser = argparse.ArgumentParser(description='Benchmark Elastic Search with various aggregation queries.')
    parser.add_argument('-t', '--test', dest='test', action='store_true', help='Reduced scenario for testing')
    args = parser.parse_args()
    
    if args.test:
        #####################
        test(cnx,dr_config) #
        #####################
    else: 
        ######################
        bench(cnx,dr_config) #
        ######################
        
        
    cnx.close()


