#!/usr/bin/env python

from operator import itemgetter
import sys
import ast


def isTenPercentMissing(nbrOfBlanks, totalcount):
    """ Returns true if number of values of the attribute is at least 10 percent missing """
    missingpercent = round((float(nbrOfBlanks) / float(totalcount)),2)
    if missingpercent > .10:
        return 1
    else:
        return 0

def getSkewedCountPercentage(valueCount, totalcount):
    """ Returns the percentage of each value """
    keyValuePercentage = []
    isSkewed = 0
    for k,v in valueCount.iteritems():
        percentOfTotal = round((v/float(totalcount)) * 100,2)
        #print "percent of skewness for total count = %s with attrvalues=%s and total=%s is %s" % (totalcount,k,v,percentOfTotal)
        if (percentOfTotal > 75):
            isSkewed = 1    #value is considered to be skewed if the count of that value is more than 75 percentage of the total
        keyValuePercentage.append('%s,%s,%s|' % (str(k),str(v),str(percentOfTotal)))
    valueCountPercentage = ''.join(keyValuePercentage)
    skewedCountPercentage = "%s~%s" % (isSkewed, valueCountPercentage)
    return skewedCountPercentage
     
def start_reducer():
    """ 
    This reducer takes input from the mapper that's layed out format below separated by pipe.
    Input:
        0 key
        1 no of distinct value
        2 no of blanks
        3 counts of all the values
        4 all the attribute values
        5 the count of each attribue values in a key value format, ie, kv["size"] = 5
    Output:
        For each line, output three different lines in this order
        1st line: 1st_line_id|attribute_name|no of distinct values|no of blanks|boolean of whether the value has 10% missing|boolean of whether the value is skewed --> this will be used to generate the summary page
        2nd line: 2nd_line_id|attribute_name|attribute values separate by pipe|' --> this will be used to generate the cleansed excel
        3rd line: 3rd_line_id|attribute_name|count of each value and percentage of that value to the overall count --> this will be used to generate value percentage excel sheet
    """
    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper.py
        keyvalue = line.split('|')
        key = keyvalue[0]
        nbrOfDistincts = keyvalue[1]
        nbrOfBlanks = keyvalue[2]
        totalcount = keyvalue[3]
        attrvalues = ast.literal_eval(keyvalue[4])  #evaluate to a list of attribute values
        valueCount = ast.literal_eval(keyvalue[5])  #evaluate to a dictionary list of key values
        valueCountPercentage = ""
        cleansedAttrs = ""
        keyValuePercentage = []
        isSkewed = 0
        isPercentMissed = 0

        #if numbef of missing values is more than 10 percentage, mark it. This will be be marked as a bad attribute to use for the WTE
        isPercentMissed = isTenPercentMissing(nbrOfBlanks, totalcount)

        cleansedAttrs='|'.join(attrvalues)   #separate each value by it's original separator and joining together with a pipe

        #returns whether any value is skewed percentage of each value
        skewedCountPercentage = getSkewedCountPercentage(valueCount, totalcount)
        isSkewed = skewedCountPercentage.split('~')[0] 
        valueCountPercentage = skewedCountPercentage.split('~')[1] 

        #get true percentage value
        #valueCountPercentage = round((valueCountDecPercentage * 100), 2)

        # write for summary page 
        #print '1|attr=%s|distinct=%s|blanks=%s|missinperc=%s|skew=%s|' % (key,nbrOfDistincts,nbrOfBlanks,isPercentMissed,isSkewed)
        print '1|%s|%s|%s|%s|%s|' % (key,nbrOfDistincts,nbrOfBlanks,isPercentMissed,isSkewed)

        #write out for cleansed spreadsheet
        print '2|%s|%s|' % (key,cleansedAttrs)

        #write out for individual attribute sheet 
        print '3|%s|%s' % (key,valueCountPercentage)

if __name__ == "__main__":
    start_reducer()
