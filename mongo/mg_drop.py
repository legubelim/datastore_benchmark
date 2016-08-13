#!/usr/bin/env python

#################################
### Drops the MongoDB collection
#################################

import pymongo
import sys

def drop_collection(client, config):
    """Delete collection

    Parameters:
      client: MongoDB client
      config: Some settings (including the name of the collection)

    """

    # retrieving the collection object
    db=client[config.db] 
    col=db[config.collection]
    
    sys.stderr.write( "Dropping the collection %s.%s\n" % (config.db,config.collection))
    ############
    col.drop() #
    ############
    sys.stderr.write( "Collection dropped\n")
    

########
# MAIN #    
########

if __name__ == '__main__':

    import mg_config # All our config variables
    
    import argparse

    # Parsing arguments
    parser = argparse.ArgumentParser(description='Drops MongoDB collection.')
    args = parser.parse_args()
    
    # Connection to MongoDB
    sys.stderr.write( "Connecting to %s on port %d\n" % (mg_config.host,mg_config.port))
    client=pymongo.MongoClient(mg_config.host,mg_config.port)

    ####################################
    drop_collection(client, mg_config) #
    ####################################

    

    


    
