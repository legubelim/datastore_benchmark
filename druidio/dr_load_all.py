#!/usr/bin/env python

######################################
### Submit ingestion task to Druid IO
######################################

import json
import httplib
import sys
import os

def load_path(cnx, config, path):
    """Load content of directory"""

    # in case the input path is a directory (and not a file name of file name pattern)
    if os.path.isdir(path):
        path=path+'/*.gz'

    sys.stderr.write( "Loading data files from %s\n" % path )

    # Build ingest JSON to be submitted
    ##################################################
    ingest=config.getIngest(path,config.data_source) #
    ##################################################
    ingest_json=json.dumps(ingest)

    # Submit the task
    headers = {'Content-type': 'application/json'}
    ##########################################################
    cnx.request("POST",config.load_url,ingest_json, headers) #
    ##########################################################
    resp=cnx.getresponse()
    sys.stderr.write( resp.read()+"\n")
    if resp.status != httplib.OK:
        raise Exception("ERROR when loading data fron " + path + ": %d %s" % (resp.status, resp.reason))
    
    sys.stderr.write( "Task submitted\n" )
    sys.stderr.write( "  Druid IO console:             http://%s:8081/#/\n" % config.host )
    sys.stderr.write( "  Druid IO ingestion follow-up: http://%s:%d/console.html\n" % (config.host,config.load_port) )




########
# MAIN #    
########

if __name__ == '__main__':

    # All our config variables
    import dr_config

    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Submit a Druid IO ingestion task to load data from CSV file(s).')
    parser.add_argument('data_path', metavar='data_path', help='ABSOLUTE path which data are to be loaded from. Can be a directory, file name, a file name pattern or a list of file names separated with comas')
    args = parser.parse_args()

    # Connection to Druid IO server
    sys.stderr.write( "Connecting to %s on port %d\n" % (dr_config.host,dr_config.load_port))
    cnx=httplib.HTTPConnection(dr_config.host,dr_config.load_port)
    cnx.connect()

    ###########################################
    load_path(cnx, dr_config, args.data_path) #
    ###########################################


    cnx.close()
    
