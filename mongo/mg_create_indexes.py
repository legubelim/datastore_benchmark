#!/usr/bin/env python

############################
### Creates MongoDB indexes
############################


import pymongo
import sys

def create_indexes(client,config, indexes, force=False):
    """ Creates MongoDB indexes

    Parameters:
      client:  MongoDB client
      config:  Some settings (including the name of the collection)
      indexes: List of indexes to be created
      force:   All existing indexes are dropped before creating the new ones

    """

    db=client[config.db]  
    col=db[config.collection]

    # Force option first drops the existing indexes
    if force:
        sys.stderr.write( "Dropping indexes for collection %s.%s\n" % (config.db,config.collection))
        ####################
        col.drop_indexes() #
        ####################
        sys.stderr.write( "  Indexes dropped\n")
        
        
    sys.stderr.write( "Creating indexes for collection %s.%s\n" % (config.db,config.collection))
        
    for index in indexes:
        sys.stderr.write( "  Creating index: %s\n" % index )
        ###########################################
        col.create_index( index, background=True) # background true to avoid locking the database
        ###########################################
        sys.stderr.write( "  Index created\n")

    sys.stderr.write( "Indexes creation finished successfully\n")



########
# MAIN #    
########


if __name__ == '__main__':

    import mg_config # All our config variables
    
    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Creates all indexes for MongoDB.')
    parser.add_argument('-f', '--force', dest='force', action='store_true',
                        help='drops all indexes before starting')
    args = parser.parse_args()


    # Connection to MongoDB
    sys.stderr.write( "Connecting to %s on port %d\n" % (mg_config.host,mg_config.port))
    client=pymongo.MongoClient(mg_config.host,mg_config.port)

    ##################################################################
    create_indexes(client, mg_config, mg_config.indexes, args.force) #
    ##################################################################
    


    
