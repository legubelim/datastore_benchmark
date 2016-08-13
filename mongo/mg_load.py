#!/usr/bin/env python

############################################
### Load content of a CSV file into MongoDB
############################################


import json
import pymongo
import gzip
import os
import datetime
import time
import sys



def load_file(client, config, data_filename):
    """Decode a gzipped CSV file and load into MongoDB

    Use bulk inserts
    Record times for every bulk: both time to read/decode data from file and time to insert the bulk
    Times are provided on stdout with format:
      LOAD, <bulk buffer size>, <time to read/decode in ms>, <time to insert the bulk in ms>

    Parameters:
      client: MongoDB client
      config: Some settings (including the name of the collection)
      data_filename: name of the file to be decoded/loaded

    """

    db=client[config.db]
    col=db[config.collection]

    def post_bulk(posts):
        """ship a bulk to MongoDB for insert"""

        # Bulk insert
        ###############################
        result=col.insert_many(posts) #
        ###############################


    mongo_posts=[]
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
                ###########################
                mongo_posts.append(input) #
                ###########################

            except Exception, e: # If we don't manage to read the line we just ignore it...
                sys.stderr.write( "WARNING: unable to decode line: %s \n" %line)
                sys.stderr.write( str(e) )
            
            # When the buffer reaches the max size (config) then we load
            if bulk_buffer_size >= config.bulk_buffer_size_max:

                batch_nb+=1
                sys.stderr.write( "  Loading the batch of data #%d (%d items)\n" % (batch_nb,bulk_buffer_size))
                time2=datetime.datetime.now() # recording time before inserting
                read_time=int((time2-time1).total_seconds()*1000) # reading/decoding time

                ########################
                post_bulk(mongo_posts) #
                ########################

                time3=datetime.datetime.now()# recording time after inserting
                post_time=int((time3-time2).total_seconds()*1000) # insert time

                # print statistics in stdout
                #################################################################
                print "LOAD: %d,%d,%d" % (bulk_buffer_size,read_time,post_time) #
                #################################################################

                # bulk variable reset
                mongo_posts=[]
                bulk_buffer_size=0
                time1=datetime.datetime.now()


        # If there are remaining unloaded items in the buffer we load them
        if bulk_buffer_size>0:

            batch_nb+=1
            sys.stderr.write( "  Loading the batch of data #%d (%d items)\n" % (batch_nb,bulk_buffer_size))
            time2=datetime.datetime.now() # recording time before inserting
            read_time=int((time2-time1).total_seconds()*1000) # reading/decoding time
            
            ########################
            post_bulk(mongo_posts) #
            ########################
                        
            time3=datetime.datetime.now()# recording time after inserting
            post_time=int((time3-time2).total_seconds()*1000) # insert time
            
            # print statistics in stdout
            #################################################################
            print "LOAD: %d,%d,%d" % (bulk_buffer_size,read_time,post_time) #
            #################################################################
        
    sys.stderr.write( "Load from %s finished sucessfully " % data_filename)
    sys.stderr.write( "  (%d items in %d batches)\n" % (item_nb,batch_nb))

    return




########
# MAIN #    
########

if __name__ == '__main__':
        
    # All our config variables
    import mg_config

    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Load data from CSV file into MongoDB.')
    parser.add_argument('data_filename', metavar='data_filename', help='data file which data are to be loaded from')
    args = parser.parse_args()

    # Connection to MongoDB
    sys.stderr.write( "Connecting to %s on port %d" % (mg_config.host,mg_config.port))
    client=pymongo.MongoClient(mg_config.host,mg_config.port)

    ##################################################
    load_file(client, mg_config, args.data_filename) #
    ##################################################


