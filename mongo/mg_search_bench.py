#!/usr/bin/env python

#################################################################
### MongoDB benchmark with various set of aggregation queries ###
#################################################################
# This code is quite specific to our data (travel searches)


import pymongo
import datetime
import sys 

##########################
## Queries definitions ###
##########################

########################################################################################
# top 10 destinations (with nb of searches) for a given market, origin and search month
def get_top_dest_for_market_and_origin_and_smonth(mkt, origin, search_month, sampling):

    topN=10
    query={"Market":mkt,
           "OriginCity": origin,
           "SearchMonth": search_month,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "DestinationCity" : "$DestinationCity" },
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"sum_count" : -1 }}
    limit={ "$limit" : topN }

    # return the pipeline
    return [match, group1,sorting,limit]
########################################################################################




########################################################################################
# top 10 OnD (with nb of searches) for a given market and search month
def get_top_ond_for_market_and_smonth(mkt, search_month, sampling):

    topN=10
    query={"Market":mkt,
           "SearchMonth": search_month,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "OnD" : "$OnD" },
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"sum_count" : -1 }}
    limit={ "$limit" : topN }

    # return the pipeline
    return [match, group1,sorting,limit]
########################################################################################




########################################################################################
# top 10 OnD (with nb of searches) for a given market and departure month
def get_top_ond_for_market_and_dmonth(mkt, dep_month, sampling):

    topN=10
    query={"Market":mkt,
           "DepartureMonth": dep_month,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "OriginCity" : "$OriginCity", "DestinationCity" : "$DestinationCity" },
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"sum_count" : -1 }}
    limit={ "$limit" : topN }

    # return the pipeline
    return [match, group1,sorting,limit]
########################################################################################




########################################################################################
# nb of searches per departure date for a given market and search month
def get_per_dep_date_for_market_and_smonth(mkt, search_month, sampling):

    query={"Market":mkt,
           "SearchMonth": search_month,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "DepartureDate" : "$DepartureDate"},
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"_id.DepartureDate" : 1 }}

    # return the pipeline
    return [match, group1,sorting]
########################################################################################




########################################################################################
# nb of searches per search date for a given market and departure month
def get_per_search_date_for_market_and_dmonth(mkt, dep_month, sampling):

    query={"Market":mkt,
           "DepartureMonth": dep_month,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "SearchDate" : "$SearchDate"},
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"_id.SearchDate" : 1 }}

    # return the pipeline
    return [match, group1,sorting]
########################################################################################




########################################################################################
# nb of searches per stay duration for a given market and search month
def get_per_stay_for_market_and_ond_and_smonth(mkt, origin, destination, search_month, sampling):

    topN=10
    query={"Market":mkt,
           "OriginCity": origin,
           "DestinationCity": destination,
           "SearchMonth": search_month,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "StayDuration" : "$StayDuration"},
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"_id.StayDuration" : 1 }}

    # return the pipeline
    return [match, group1,sorting]
########################################################################################




########################################################################################
# nb of searches per day of the week for a given market, origin country and destinatin country
def get_per_depdow_for_market_and_ctries(mkt, origin_ctry, destination_ctry, sampling):

    topN=10
    query={"Market":mkt,
           "OriginCountry": origin_ctry,
           "DestinationCountry": destination_ctry,
           "Sampling": {"$lte":sampling},
       }
    match={ "$match": query }
    group1={"$group": 
            { "_id" : { "DepartureWeekDay" : "$DepartureWeekDay"},
              "sum_count": { "$sum" : "$Count" }
          }
        }
    sorting={ "$sort" : {"_id.DepartureWeekDay" : 1 }}

    return [match, group1,sorting]
########################################################################################





##################
### Run Search ###
##################

def run_search(col, search_name,routing,  query_obj):
    """ runs a search and records some stats on stdout

    Stats are on the following format:
      SEARCH: <name of the search>, <routing>, <nb of returned docs, <search time>

    Parameters:
      col:    MongoDB collection
      search_name: name of the search (to identify it in the recorded stats)
      query_json:  query to be ran
      routing:     routing key (empty string if none)
      filter_path: "JSON paths" to be returned by elastic search (that lightens the output)

    """


    # Sending Search request to MongoDB and printing the perf
    sys.stderr.write( "Running search %s\n" % search_name)
    time1=datetime.datetime.now()

    # max number of results to fetch
    max_result=99999

    # definition of the cursor
    #################################
    cursor=col.aggregate(query_obj) #
    #################################

    ### Following commented code prints the execution plan! ###
    #import pprint
    #pp = pprint.PrettyPrinter()
    #pp.pprint(db.command('aggregate','small', pipeline=query_obj, explain=True))
  
    # fetch the data - We don't do anything with the result: we just want to record the time
    nbrows=0
    docs=[]
    for doc in cursor:
        docs.append(doc)
        nbrows+=1
        #print doc
        if nbrows>max_result: break
        
    time2=datetime.datetime.now()
    
    # The following print records KPIs to be gathered to build the benchmark results
    ##########################################################################################################
    print "SEARCH: %s,%s,%d,%d" % (search_name, routing, len(docs),int((time2-time1).total_seconds()*1000) ) #
    ##########################################################################################################



############
### TEST ###
############

def test(client, config, sampling=100):
    """ Reduced benchmark secnario set
    
    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings (including the name of the collection)
      sampling: sampling ratio to be applied (between 1 and 100)

    """

    # Retrieving MongoDB collection
    db=client[config.db]
    col=db[config.collection]

    sys.stderr.write("Starting minimal benchmark scenarios for testing purpose\n")

    run_search(col,"TopDestForMktOriSMonth-%d"%sampling,"FR",get_top_dest_for_market_and_origin_and_smonth("FR","PAR","2015-01",sampling))
    run_search(col,"TopOnDForMktSMonth-%d"%sampling,"FR",  get_top_ond_for_market_and_smonth("FR","2015-01",sampling))
    run_search(col,"TopOnDForMktDMonth-%d"%sampling,"FR",  get_top_ond_for_market_and_dmonth("FR","2015-10",sampling))
    run_search(col,"PerDDateForMktSMonth-%d"%sampling,"FR",  get_per_dep_date_for_market_and_smonth("FR","2015-01",sampling))
    run_search(col,"PerSDateForMktDMonth-%d"%sampling,"FR",  get_per_search_date_for_market_and_dmonth("FR","2015-10",sampling))
    run_search(col,"PerStayForMktOnDSMonth-%d"%sampling,"FR",  get_per_stay_for_market_and_ond_and_smonth("FR","PAR","NYC","2015-01",sampling))
    run_search(col,"PerDDOWForMktCtries-%d"%sampling,"FR",get_per_depdow_for_market_and_ctries("FR","FR","US",sampling))

    sys.stderr.write("Minimal benchmark scenarios finished\n")
    
  

#############
### BENCH ###
#############

def bench(client, config):
    """Benchmark based on various aggregation queries.

    Parameters:
      client: MongoDB client
      config: Some settings (including the name of the collection)

    """


    # Retrieving MongoDB collection
    db=client[config.db]
    col=db[config.collection]


    ### creating some input values ###

    samplings=[100,20,4]
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
        run_search(col,"TopOnDForMktSMonth-%d"%sampling,mkt,get_top_ond_for_market_and_smonth(mkt,search_month, sampling))
        run_search(col,"PerDDateForMktSMonth-%d"%sampling,mkt,get_per_dep_date_for_market_and_smonth(mkt,search_month,sampling))
    # Creating all the possible input combinations: market, dep month, sampling
    test_cases = list(itertools.product(mkts,dep_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,dep_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,dep_month, sampling))+"\n")
        run_search(col,"TopOnDForMktDMonth-%d"%sampling,mkt,get_top_ond_for_market_and_dmonth(mkt,dep_month,sampling))
        run_search(col,"PerSDateForMktDMonth-%d"%sampling,mkt,get_per_search_date_for_market_and_dmonth(mkt,dep_month,sampling))

    # Creating all the possible input combinations: market, origin, search_month, sampling
    test_cases = list(itertools.product(mkts,origins,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin,search_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin,search_month,sampling))+"\n")
        run_search(col,"TopDestForMktOriSMonth-%d"%sampling,mkt,get_top_dest_for_market_and_origin_and_smonth(mkt,origin, search_month,sampling))

    # Creating all the possible input combinations: market, origin, destination, search_month, sampling
    test_cases = list(itertools.product(mkts,origins,destinations,search_months, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin,destination,search_month,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin,destination,search_month,sampling))+"\n")
        run_search(col,"PerStayForMktOnDDMonth-%d"%sampling,mkt,get_per_stay_for_market_and_ond_and_smonth(mkt,origin,destination, search_month,sampling))


    # Creating all the possible input combinations: market, origin country, destination country, sampling
    test_cases = list(itertools.product(mkts,origin_ctries,dest_ctries, samplings))
    random.shuffle(test_cases)
    # Looping over all input combinations
    for (mkt,origin_ctry,destination_ctry,sampling) in test_cases:
        sys.stderr.write( str((mkt,origin_ctry,destination_ctry,sampling))+"\n")
        run_search(col,"PerDDOWForMktCtries-%d"%sampling,mkt,get_per_depdow_for_market_and_ctries(mkt ,origin_ctry,destination_ctry,sampling))

    sys.stderr.write("Benchmark scenarios finished\n")

########
# MAIN #    
########

if __name__ == '__main__':

    # All our configuration variables
    import mg_config

    import argparse 

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Benchmark Elastic Search with various aggregation queries.')
    parser.add_argument('-t', '--test', dest='test', action='store_true', help='Reduced scenario for testing')
    args = parser.parse_args()
    
    # Connection to MongoDB
    sys.stderr.write( "Connecting to %s on port %d" % (mg_config.host,mg_config.port))
    client=pymongo.MongoClient(mg_config.host,mg_config.port)
    
    if args.test:
        ########################
        test(client,mg_config) #
        ########################
    else: 
        #########################
        bench(client,mg_config) #
        #########################
    
