#!/usr/bin/env python

#####################################################
### Delete the content of a file from Elastic Search 
#####################################################

import json
import httplib
import os
import sys


def delete_file(cnx, config, data_filename):
    """Clean Elastic Search index from all data flagged with given filename

    It consists in running a search query to fetch all data with given data file in order to get their IDs and to delete them one by one (through bulk deletes though)

    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings
      data_filename: name of the file to be decoded/loaded

    """


    def post_bulk(bulk_json):
        """Ship a json bulk to Elastic Search"""

        # Bulk insert
        ####################################################################
        cnx.request("POST",config.index+"/"+config.typ+"/_bulk",bulk_json) #
        ####################################################################

        # Get and read response from Elastic Search server
        resp=cnx.getresponse()
        resp_msg_json= resp.read()
        resp_msg=json.loads(resp_msg_json)
        # Check status: both HTTP and within the Elastic Search answer
        if resp.status != httplib.OK or resp_msg["errors"] is True:
            sys.stderr.write( bulk_json)
            sys.stderr.write( resp_msg_json)
            raise Exception("ERROR when bulk deleting from " + config.index+"/"+typ + ": %d %s" % (resp.status, resp.reason))


            
    sys.stderr.write( "Scrolling and deleting data from file " + data_filename +"\n" )


    #############################################
    ### QUERY to retrieve items to be deleted ###
    query_json='''                              
    {                                           
    "query" : {
        "constant_score" : { 
            "filter" : {
                "term" : { 
                    "dataFile" : "%s"
                }
            }
        }
    },
    "size":%d
    }
    ''' % (os.path.basename(data_filename),
           config.bulk_buffer_size_max)
    ##############################################


    # To ensure well formed JSON we decode the JSON and re-encode it
    query=json.loads(query_json)
    query_json=json.dumps(query)
    sys.stderr.write(query_json+"\n")
    
    # Sending Search request (with scrolling) to Elastic Search 
    sys.stderr.write( "  Retrieving first batch of data\n")
    ################################################################################
    cnx.request("GET",config.index+"/"+config.typ+"/_search?scroll=1m",query_json) #
    ################################################################################
    resp=cnx.getresponse()
    reply_json=resp.read()
    reply=json.loads(reply_json)
    #print reply
    if resp.status != httplib.OK:
        raise Exception("ERROR when retrieving data of index %s: %d %s" % (config.index,resp.status, resp.reason))

        
    ### Next step is to scroll at the data and to prepare and post corresponding bulk deletes

    # Action to put in bulk delete
    bulk_action={} 
    bulk_action["delete"]={}
    # Counters
    batch_nb=0
    deleted_rows=0

    # Loop on scrolled batches 
    while True:

        batch_nb+=1

        # Buffer of JSON command for bulk delete - reset
        elastic_post=""
        # reset counter
        rows_in_batch=0
        
        # hits are returned data from the scroll
        hits=reply["hits"]["hits"]
        # If the batch is empy then it's over we can stop
        if len(hits)==0: break

        # Loop over the items of the batch and build the delete bulk
        for hit in hits:

            # Add in the buffer: 
            # One Json line for the action (includes the routing/sharding info)
            # The routing will drive the partitionning of the data
            ###################################################
            bulk_action["delete"]["_routing"]=hit["_routing"] #
            bulk_action["delete"]["_id"]=hit["_id"]           #
            bulk_action_json=json.dumps(bulk_action)          #
            elastic_post+=bulk_action_json+"\n"               #
            ###################################################

            rows_in_batch+=1

        # Delete bulk
        deleted_rows+=rows_in_batch
        sys.stderr.write( "  Deleting the batch of data #%d (%d items)\n" % (batch_nb, rows_in_batch))
        post_bulk(elastic_post)

        # Fetching the next set of data
        sys.stderr.write( "  Retrieving next batch of data\n")

        ###################################################################
        cnx.request("GET","_search/scroll?scroll=1m",reply["_scroll_id"]) #
        ###################################################################

        resp=cnx.getresponse()
        reply_json=resp.read()
        reply=json.loads(reply_json)
        if resp.status != httplib.OK:
            raise Exception("ERROR when retrieving data of index %s: %d %s" % (config.index,resp.status, resp.reason))


    sys.stderr.write( "Delete from " + data_filename + " finished sucessfully: %d batches (%d items)\n" % (batch_nb,deleted_rows))
    


########
# MAIN #    
########

if __name__ == '__main__':

    # All our config variables
    import el_config

    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Delete data flagged with a filename from Elastic Search and force refresh (forcemerge).')
    parser.add_argument('data_filename', metavar='data_filename', help='name of the data file which data are to be removed')
    args = parser.parse_args()

    # Connection to Elastic Search server
    sys.stderr.write( "Connecting to " + el_config.host + " on port %d\n" % el_config.port)
    cnx=httplib.HTTPConnection(el_config.host,el_config.port)
    cnx.connect()

    #################################################
    delete_file(cnx, el_config, args.data_filename) #
    #################################################

    # Forcing merge I.e. forcing pending inserts/deletes to be integrated in index
    sys.stderr.write( "Forcing merge\n")
    cnx.request("POST",el_config.index+"/_forcemerge?max_num_segments=5")
    resp=cnx.getresponse()
    sys.stderr.write( resp.read()+"\n")
    if resp.status != httplib.OK:
        raise Exception("ERROR when changing forcing merge for " + el_config.index + ": %d %s" % (resp.status, resp.reason))
    sys.stderr.write( "Forcemerge successful\n")

    cnx.close()


