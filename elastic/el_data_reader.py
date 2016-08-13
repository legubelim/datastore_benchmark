#!/usr/bin/env python

###############################
### Travel Search data decoder
###############################
# Decoding function for the Travel Search data sample

import datetime


### Helpers ###

# Conversion from String to Integer
def toInt(string, default=0):
    try: out=int(string)
    except: out=default
    return out

# Conversion from string to Float
def toFloat(string, default=0.0):
    try: out=float(string)
    except: out=default
    return out

# Conversion from string to Boolean
def toBool(string, default=False):
    if string=="False" or string=="false": return False
    if string=="True"  or string=="true" : return True
    try: out=bool(string)
    except: out=default
    return out


### Decoding function ###

def decode_search_line(line):
    """Decodes a CSV search

    Parameters:
      line: CSV line string
    
    """

    fields=line.rstrip().split("^")
    
    i=0
    search={}
    search["Market"]=fields[i]                        # Country where the search was issued
    i+=1; search["MarketContinent"]=fields[i]
    i+=1; search["SearchDate"]=fields[i]              # Date when the search was issued
    i+=1; search["SearchWeekDay"]=fields[i]
    i+=1; search["SearchWeek"]=fields[i]
    i+=1; search["SearchMonth"]=fields[i]
    i+=1; search["SearchQuarter"]=fields[i]
    i+=1; search["OriginCity"]=fields[i]              # Origin city of the requested trip
    i+=1; search["OriginCountry"]=fields[i]
    i+=1; search["OriginContinent"]=fields[i]
    i+=1; search["DestinationCity"]=fields[i]         # Destination city of the requested trip
    i+=1; search["DestinationCountry"]=fields[i]
    i+=1; search["DestinationContinent"]=fields[i]
    i+=1; search["OnD"]=fields[i]                     # Origin and destination concatenated
    i+=1; search["DepartureDate"]=fields[i]           
    i+=1; search["DepartureWeekDay"]=fields[i]
    i+=1; search["DepartureWeek"]=fields[i]
    i+=1; search["DepartureMonth"]=fields[i]
    i+=1; search["DepartureQuarter"]=fields[i]
    i+=1; search["AdvancePurchase"]=toInt(fields[i])  # Number of days between search date and departure date
    i+=1; search["Distance"]=toFloat(fields[i])       # Distance between Origin and Destination
    i+=1; search["Geography"]=fields[i]               # D:Domestic, R:Regional (same continent), I:International 
    i+=1; search["Count"]=toInt(fields[i])            # Number of travel searches
    i+=1; search["Sampling"]=toInt(fields[i])         # random number between 0 and 99
    i+=1; search["IsOneWay"]=toBool(fields[i])        # True if One Way trip
    i+=1; search["StayDuration"]=toInt(fields[i],-1)  # Number of days between departure date and return date
    if not search["IsOneWay"]:
        i+=1; search["ReturnDate"]=fields[i]
        i+=1; search["ReturnWeekDay"]=fields[i]
        i+=1; search["ReturnWeek"]=fields[i]
        i+=1; search["ReturnMonth"]=fields[i]
        i+=1; search["ReturnQuarter"]=fields[i]

    return search


def printExamples():
    """ Some manual test """

    print "Examples of travel search data decoding"
    
    # Round trip example
    test_RT="FR^EU^2015-05-04^Mon^2015-18^2015-05^2015-2^BOG^CO^SA^PAR^FR^EU^BOGPAR^2015-05-11^Mon^2015-19^2015-05^2015-2^False^2015-06-18^Thu^2015-24^2015-06^2015-2^7^38^8627.9^I^3^49"
    print "\nDecoding CSV line: \n%s\ngives:" % test_RT
    print decode_search_line(test_RT)

    # One way example
    test_OW="FR^EU^2015-05-04^Mon^2015-18^2015-05^2015-2^JKT^ID^AS^PDG^ID^AS^JKTPDG^2015-05-17^Sun^2015-19^2015-05^2015-2^True^^^^^^13^-1^954.1^D^2^94"
    print "\nDecoding CSV line: \n%s\ngives:" % test_OW
    print decode_search_line(test_OW)


# Not supposed to be called directly
if __name__ == '__main__':

    printExamples()




