#!/usr/bin/env python

import sys
import difflib
import logging

def set_logger():
    global logger
    logfile = "stringmatch.log"

    FORMAT = "[%(asctime)s:%(filename)s:%(levelname)s:%(funcName)10s():%(lineno)d] %(message)s"
    logging.basicConfig(filename=logfile,filemode='w',format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)

    #logger.debug('this is a debug level message')
    #logger.info('this is an info level  message')
    #logger.warning('this is a warning level message')
    #logger.error('this is an error level message')
    #logger.critical('this is a critical level message')

def get_frequent_value(similarvaluelist):
    """ Return the value that appears the most in the list """
    current_value = None
    valuetouse = None
    current_counter = 0
    next_counter = 0
    for value in similarvaluelist:
        for value2 in similarvaluelist:
            if (value2 == value):
                next_counter += 1
        
        if (next_counter > current_counter):
            valuetouse = value
            current_counter = next_counter
        next_counter = 0
    #print "pick value from = %s, return value = %s" % (similarvaluelist,valuetouse)
    return valuetouse 

def create_similarvalue_list(words, similarWord):
    """ Given a line of words, put in a list of all the words that are 95% match in similarity """
    myindex = 0
    firstword= True
    similarValueList=[]
    #create similar value list to get most frequent appeared value 
    for eachValue in words:
        #skip the first column because it is a key
        if firstword:
            firstword = False
            continue

        myindex += 1
        
        #skip all previous value.  
            #*This code is comment out because in order to get the frequent value for every value, 
            #*it has to be compared again to all previous values
        #if (myindex < index):
        #    continue;

        #apply string matching, if meet threshold, then words are the same
        matchPercentage = difflib.SequenceMatcher(None, eachValue, similarWord).ratio()
        #print "match percentage between %s and %s is %f" % (eachValue, similarWord, round(matchpercentage,2))
        if (matchPercentage > .95):
            #print "adding to similar value list for word : %s" % word
            similarValueList.append(eachValue)
    return similarValueList

def isValueCompared(compareValue, alreadyCommparedValues):
    """ Returns True if the passed in word has already been compared previously """
    if (len(alreadyCommparedValues) != 0):
        for eachValue in alreadyCommparedValues:
            matchpercentage = difflib.SequenceMatcher(None, eachValue, compareValue).ratio()
            #print "match percentage between %s and %s is %f" % (word, eachValue, round(matchpercentage,2))
            if (matchpercentage > .95):
                #isValueExcluded = True
                return True
                #print "match percentage is %s, breaking out" % matchpercentage
                #break
        else:   #this gets executed upon exhausting of for loop, but not from break statement
            #if list is exhausted without finding any word, then it must be a new word, increase distinct
            #if (isBlankWord == False):
            #    distinctcount += 1
            return False
    else:
        #if (isBlankWord == False):
        #    distinctcount += 1
        return False

def start_mapper():
    """
    Take in the standard input, run through the Ratcliff/Obershelp string match algorithm, which does similarity metrics with spelling errors detection
    Refer here for more information on different string match algorithm: http://www.morfoedro.it/doc.php?n=223&lang=en
    """
    # input comes from STDIN (standard input)
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        lines = line.split("\n")

        key = ""
        value = "" 
        alreadyCommparedValues = []
        similarvaluelist = []
        attrvaluelist = []
        matchpercentage = 0
        blankcount = 0
        distinctcount = 0
        totalcount = 0
        firstword = 0
        valuecountdict = {}        #keeps track of the number of times for each value
        isValueExcluded = False
        isBlankWord = False
        index = 0
        for myline in lines:
            totalcount = 0
            words = myline.split("|")
            for word in words:
                isBlankWord = False
                firstword2 = 0
                #get all the similar values for that particular word
                #print "current word is %s" % word

                #first word is the key, so skip the first word
                if (firstword == 0):
                    #print "key is: %s. Skipping loop" % word
                    key = word
                    firstword = 1
                    continue
                index += 1
               
                #count the number of blanks
                if (word == ""):
                    #print "found blank word, increasing blank count"
                    isBlankWord = True 
                    blankcount += 1
                
                totalcount += 1     #count of all attribute values

                #get the list of similarity words for each word
                similarValueList = create_similarvalue_list(words, word)
                
                #get the most frequently appeared value
                attrvalue = get_frequent_value(similarValueList)

                #count the number of distinct value
                #keeps a list of all the values that has already been compared, if the next value is not in the list, then it's distinct
                if not isValueCompared(word, alreadyCommparedValues):
                    #if it's a new comparision value, then get the count of the number of the numbef of times for that value
                    valuecountdict[attrvalue] = len(similarValueList) 
                    if not isBlankWord:
                        distinctcount += 1
                

                #value has already been compared, add to exclusion list so not to compare again
                alreadyCommparedValues.append(word) 
                
                #add each value to the list to generate a clean list
                #print "most frequent value is %s" % attrvalue
                attrvaluelist.append(attrvalue)
                similarvaluelist = []
                isValueExcluded = False
            
            #write out for the summary
            value = "%s|%s|%s|%s|%s" % (str(distinctcount), str(blankcount), totalcount, attrvaluelist, valuecountdict)
            print "%s|%s" % (key, value)

if __name__ == "__main__":
    start_mapper()

