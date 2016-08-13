#!/usr/bin/env python

###################################################
### Load content of a CSV file into Elastic Search 
###################################################


import json
import httplib
import gzip
import os
import datetime
import random
import time
import sys





def load_file(cnx, config, data_filename):
    """Decode a gzipped CSV file and load into Elastic Search

    Use bulk inserts
    Record times for every bulk: both time to read/decode data from file and time to insert the bulk
    Times are provided on stdout with format:
      LOAD, <bulk buffer size>, <time to read/decode in ms>, <time to insert the bulk in ms>

    Parameters:
      cnx:    HTTP connection to Elastic Search
      config: Some settings
      data_filename: name of the file to be decoded/loaded

    """

    def post_bulk(bulk_json):
        """ship a json bulk to Elastic Search for insert"""

        nbtry=0
        success=False

        # Bulk insert
        ####################################################################
        cnx.request("POST",config.index+"/"+config.typ+"/_bulk",bulk_json) #
        ####################################################################

        # Get and read response from Elastic Search server
        resp=cnx.getresponse()
        resp_msg_json= resp.read()
        #sys.stderr.write( resp_msg_json + "\n")
        resp_msg=json.loads(resp_msg_json)
        # Check status: both HTTP and within the Elastic Search answer
        if resp.status != httplib.OK or resp_msg["errors"] is True:
            sys.stderr.write( bulk_json)
            sys.stderr.write( resp_msg_json +"\n")
            raise Exception("ERROR when bulk loading into %s/%s: %d %s\n" % (config.index,config.typ, resp.status, resp.reason))

    sys.stderr.write( "Starting loading data from %s\n" % data_filename )

    # opening data file
    with gzip.open(data_filename) as f:

        item_nb=0 # item number in the file
        batch_nb=0 # number of the batch within the file

        # bulk variables reset
        bulk_action={}
        bulk_action["create"]={}
        elastic_post=""
        bulk_buffer_size=0 # item number in the batch
        time1=datetime.datetime.now() # recording time before reading/decoding

        # Reading the file line by line
        for line in f:
            
            # decode the CSV line - If we don't manage to read the line we just ignore it...
            try:
                item_nb+=1

                # reading/decoding line
                ######################################
                input=config.decode_input_line(line) #
                ######################################
                bulk_buffer_size+=1
                
                # dataFile field is not really useful for analytics 
                # but it allows keeping track (and potentially cleaning) what was loaded
                input["dataFile"]=os.path.basename(data_filename)
                # itemNb allow to find back which line of the file corresponds to which document in the database
                input["itemNb"]=item_nb-1
                
                # convert into JSON
                input_json=json.dumps(input)
 
                # Add in the buffer: 
                # One Json line for the action (includes the routing/sharding info)
                # The routing will drive the partitionning of the data
                # Don't forget to specify it when running inputes
                ############################################################
                bulk_action["create"]["_routing"]=config.getRouting(input) ### ROUTING KEY IS DEFINED HERE ###
                bulk_action_json=json.dumps(bulk_action)                   #
                # One Json line for the input itself                       #
                elastic_post+=bulk_action_json+"\n"+input_json+"\n"        #
                ############################################################

            except Exception, e: # If we don't manage to read the line we just ignore it...
                sys.stderr.write( "WARNING: unable to decode line: %s \n" %line)
                sys.stderr.write( str(e) )
            
            # When the buffer reaches the max size (config) then we load
            if bulk_buffer_size >= config.bulk_buffer_size_max:

                batch_nb+=1
                sys.stderr.write( "  Loading the batch of data #%d (%d items)\n" % (batch_nb,bulk_buffer_size))
                time2=datetime.datetime.now() # recording time before inserting
                read_time=int((time2-time1).total_seconds()*1000) # reading/decoding time

                #########################
                post_bulk(elastic_post) #
                #########################

                time3=datetime.datetime.now()# recording time after inserting
                post_time=int((time3-time2).total_seconds()*1000) # insert time

                # print statistics in stdout
                #################################################################
                print "LOAD: %d,%d,%d" % (bulk_buffer_size,read_time,post_time) #
                #################################################################

                # bulk variable reset
                elastic_post=""
                bulk_buffer_size=0
                time1=datetime.datetime.now()


        # If there are remaining unloaded items in the buffer we load them
        if bulk_buffer_size>0:

            batch_nb+=1
            sys.stderr.write( "  Loading the batch of data #%d (%d items)\n" % (batch_nb,bulk_buffer_size))
            time2=datetime.datetime.now() # recording time before inserting
            read_time=int((time2-time1).total_seconds()*1000) # reading/decoding time
            
            #########################
            post_bulk(elastic_post) #
            #########################
            
            time3=datetime.datetime.now()# recording time after inserting
            post_time=int((time3-time2).total_seconds()*1000) # insert time
            
            # print statistics in stdout
            #################################################################
            print "LOAD: %d,%d,%d" % (bulk_buffer_size,read_time,post_time) #
            #################################################################
        
    sys.stderr.write( "Load from %s finished sucessfully " % data_filename)
    sys.stderr.write( "  (%d items in %d batches)\n" % (item_nb,batch_nb))

    return item_nb



########
# MAIN #    
########

if __name__ == '__main__':
        
    # All our config variables
    import el_config

    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Load data from CSV file into Elastic Search.')
    parser.add_argument('data_filename', metavar='data_filename', help='data file which data are to be loaded from')
    args = parser.parse_args()

    # Connection to Elastic Search server
    sys.stderr.write( "Connecting to " + el_config.host + " on port %d\n" % el_config.port)
    cnx=httplib.HTTPConnection(el_config.host,el_config.port)
    cnx.connect()

    ###############################################
    load_file(cnx, el_config, args.data_filename) #
    ###############################################

    cnx.close()

