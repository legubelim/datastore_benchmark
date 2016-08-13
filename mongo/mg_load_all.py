#!/usr/bin/env python

#############################################
# Load the content of a directory in MongoDB
#############################################

import mg_load # To load the content of a file

import pymongo

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

    # Connection to MongoDB
    sys.stderr.write( "Connecting to %s on port %d\n" % (mg_config.host,mg_config.port))
    client=pymongo.MongoClient(mg_config.host,mg_config.port)
    db=client[config.db]
    col=db[config.collection]

    ### loading files ###
    time1=datetime.datetime.now()
    firstFile=True

    # looping on files
    for data_file in glob.glob(data_dir+"/*.gz"):

        # if already processed then skip
        if os.path.basename(data_file) in processed_files:
            sys.stderr.write( "File %s already processed\n" % (data_file))

        else: # otherwise process
            sys.stderr.write( "Starting load of file %s\n" % (data_file))
            
            if firstFile and not force: # for the first file we first clean data that could have already been loaded
                ###########################################################
                col.delete_many({"dataFile":os.path.basename(data_file)}) # takes too long without index
                ###########################################################
                firstFile = False
            
            # load file
            ##############################################
            mg_load.load_file(client, config, data_file) #
            ##############################################
            
            # Updating tracking file
            processed_files[os.path.basename(data_file)]=True
            with open(tracking_file_name, 'a') as f:
                f.write(os.path.basename(data_file)+"\n")

            sys.stderr.write( "Load of file %s successful\n" % data_file)

    # Record the total time to load the data
    time2=datetime.datetime.now()
    merge_time=int((time2-time1).total_seconds()*1000)
    print "LOAD_ALL: %d" % (merge_time)

    sys.stderr.write( "Load from directory %s finished sucessfully\n" % data_dir)

    

# used as script we load the files in argument directory
if __name__ == '__main__':

    # All our config variables
    import mg_config

    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Load data from CSV file(s) contained in a directory into Elastic Search. In case of re-start it doesn\'t load already loaded files.')
    parser.add_argument('data_dir', metavar='data_dir', help='data directory which data are to be loaded from')
    parser.add_argument('-f', '--force', dest='force', action='store_true',
                        help='Ignore restart point')
    args = parser.parse_args()

    #####################################################
    loading_files(mg_config, args.data_dir, args.force) #
    #####################################################




