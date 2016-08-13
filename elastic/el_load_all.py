#!/usr/bin/env python

####################################################
# Load the content of a directory in Elastic Search
####################################################

import el_load # To load the content of a file
import el_delete_file # To delete the content of a file

import json
import httplib
import datetime
import sys
import glob
import os
import time

def loading_files(config, data_dir, force=False):
    """ Load the .gz files in the input directory

    Manages a restart point: loaded data files are recorded in a tracking file (written in the same directory). In case of restart the already loaded data files are ignored (except in force mode).

    Parameters:
      config: Some settings
      data_dir: name of the directory data file are t be loaded from
      force: whether the restart point should be ignored

    """

    sys.stderr.write( "Loading data files from directory %s\n" % data_dir )

    # file containing names of already processed data files
    tracking_file_name=data_dir+"/"+config.track_file
    processed_files={} # already processed files
    # In force mode we reset the tracking file
    try:
        if force: 
            os.remove(tracking_file_name)
        else:
            # otherwise get the list of already processed files
            with open(tracking_file_name) as f:
                for line in f:
                    processed_files[line.rstrip()]=True
    except IOError: pass # if no file then fine we'll create it

    # Connection to Elastic Search server
    sys.stderr.write( "Connecting to %s on port %d\n" % (config.host,config.port))
    cnx=httplib.HTTPConnection(config.host,config.port)
    cnx.connect()

    # increasing index refresh to get better perf during the load
    sys.stderr.write( "Updating index refresh interval to %ss\n" % config.load_refresh_interval)
    ##########################################################################################
    refresh_interval={"index" : {"refresh_interval" : "%ds"%config.load_refresh_interval } } #
    refresh_interval_json=json.dumps(refresh_interval)                                       #
    cnx.request("PUT",config.index+"/_settings",refresh_interval_json)                       #
    ##########################################################################################
    resp=cnx.getresponse()
    sys.stderr.write( resp.read() + "\n")
    if resp.status != httplib.OK:
        raise Exception("ERROR when changing refresh_interval for " + config.index + ": %d %s" % (resp.status, resp.reason))


    ### loading files ###
    time1=datetime.datetime.now()
    firstFile=True
    item_nb=0 # number of documents loaded

    # looping on files
    for data_file in glob.glob(data_dir+"/*.gz"):

        # if already processed then skip
        if os.path.basename(data_file) in processed_files:
            sys.stderr.write( "File %s already processed\n" % (data_file))

        else: # otherwise process
            sys.stderr.write( "Starting load of file %s\n" % (data_file))
            
            if firstFile and not force: # for the first file we first clean data that could have already been loaded
                ####################################################
                el_delete_file.delete_file(cnx, config, data_file) #
                ####################################################
                firstFile = False
            
            ####################################################
            item_nb+=el_load.load_file(cnx, config, data_file) #
            ####################################################
            
            # Updating tracking file
            processed_files[os.path.basename(data_file)]=True
            with open(tracking_file_name, 'a') as f:
                f.write(os.path.basename(data_file)+"\n")

            sys.stderr.write( "Load of file %s successful\n" % data_file)

    # Record the total time to load the data
    time2=datetime.datetime.now()
    merge_time=int((time2-time1).total_seconds()*1000)
    print "LOAD_ALL: %d,%d" % (item_nb,merge_time)

    # FORCEMERGE to ensure all pending inserts are indexed
    sys.stderr.write( "Forcing merge\n")
    time1=datetime.datetime.now()
    ####################################################################
    cnx.request("POST",config.index+"/_forcemerge?max_num_segments=5") #
    ####################################################################
    resp=cnx.getresponse()
    sys.stderr.write( resp.read()+"\n")
    if resp.status != httplib.OK:
        raise Exception("ERROR when changing forcing merge for " + config.index + ": %d %s" % (resp.status, resp.reason))

    # Record the time for the forcemerge
    time2=datetime.datetime.now()
    merge_time=int((time2-time1).total_seconds()*1000)
    print "FORCEMERGE: %d" % (merge_time)

    cnx.close()
    sys.stderr.write( "Load from directory %s finished sucessfully\n" % data_dir)

    

########
# MAIN #    
########

if __name__ == '__main__':

    # All our config variables
    import el_config

    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Load data from CSV file(s) contained in a directory into Elastic Search. In case of re-start it doesn\'t load already loaded files.')
    parser.add_argument('data_dir', metavar='data_dir', help='data directory which data are to be loaded from')
    parser.add_argument('-f', '--force', dest='force', action='store_true',
                        help='Ignore restart point')
    args = parser.parse_args()

    # There are failure sometimes: connection reset, timeout...
    # Let's be robust with that
    nb_retry=el_config.nb_retry_max
    force=args.force
    
    # In case of failure we re-try. It restarts from the last ongoing file.
    while nb_retry>0:
        nb_retry-=1
        try:
            ################################################
            loading_files(el_config, args.data_dir, force) #
            ################################################
            break
        except Exception,e: 
            # in case of problem we sleep 10s and try again
            sys.stderr.write( "WARNING: load failed - Retry in 10s \n")
            sys.stderr.write( str(e))
            time.sleep(10) 
        force=False # the restart point can only be ignored for the first try (otherwise we would always start from the beginning)


    if nb_retry == 0 :
        sys.stderr.write( "ERROR: load failed after %d retries\n" % el_config.nb_retry_max)
        exit(1)
