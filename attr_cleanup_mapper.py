#!/usr/bin/env python

import sys
import difflib
import logging
import time
import timeit
from sets import Set
import pdb

numSimComp = 0
numCalcDistComp = 0
numFreqValComp = 0
getFreqVal_totaltime = 0
createSimList_totaltime = 0
isValComp_totaltime = 0

def set_logger():
    global logger
    logfile = "attr_cleanup_mapper.log"

    FORMAT = "[%(asctime)s:%(filename)s:%(levelname)s:%(funcName)10s():%(lineno)d] %(message)s"
    logging.basicConfig(filename=logfile,filemode='w',format=FORMAT, datefmt='%m/%d/%Y %I:%M:%S %p')
    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)

    #add additional logger
    stdoutlog = logging.StreamHandler(sys.stdout)
    stdoutlog.setLevel(pyconfig.logLevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stdoutlog.setFormatter(formatter)
    logger.addHandler(stdoutlog)

    #logger.debug('this is a debug level message')
    #logger.info('this is an info level  message')
    #logger.warning('this is a warning level message')
    #logger.error('this is an error level message')
    #logger.critical('this is a critical level message')

def log_timer(msg, start_time, end_time, mode):
    global getFreqVal_totaltime
    global createSimList_totaltime
    global isValComp_totaltime

    time_in_sec=end_time-start_time
    time_in_min=time_in_sec/60

    if (mode == 1):
        sys.stderr.write("%s:\tclock: [%s] start time: [%s] end time: [%s] time elapsed: [%s sec] [%s min]\n" % (msg, time.ctime(),start_time,end_time,round(time_in_sec,2),round(time_in_min,2)))

    #get the total time for each function
    if (msg == "get_frequent_value"):
        getFreqVal_totaltime += time_in_sec 
    elif (msg == "create_similarvalue_list"):
        createSimList_totaltime += time_in_sec
    elif (msg == "isValueCompared"):
        isValComp_totaltime += time_in_sec 

def get_frequent_value(similarvaluelist):
    """ Return the value that appears the most in the list """
    global numFreqValComp
    
    currentValue = None 
    nextValue = ""
    mostFreqValue = ""
    currentCounter = 0
    nextCounter = 0
    startLoopIndex=0
    loopIndex = 0
    
    similarValueListCopy = list(similarvaluelist)
    similarValueListCopy.sort() #sort the list to lessen the amount of comparisons

    listSize = len(similarValueListCopy)
    if (listSize == 1):
        mostFreqValue = similarValueListCopy[loopIndex] #return itself if list is 1 

    #since the list is sorted, only compare the next value that is not similar 
    #while (startLoopIndex < len(similarValueListCopy)-1):
    while (startLoopIndex < listSize-1):
        nextValue = similarValueListCopy[startLoopIndex]
        if (nextValue != currentValue):
            for loopIndex in xrange(startLoopIndex, len(similarValueListCopy)):
                numFreqValComp += 1
                if (nextValue == similarValueListCopy[loopIndex]):
                    nextCounter += 1
                else:
                    break   #go to next value that is not the same
            
            #if the next word has more count, then set that that as the most frequent one
            if (nextCounter > currentCounter):
                mostFreqValue = nextValue
                currentCounter = nextCounter
        
            nextCounter=0
        else:
            loopIndex += 1 
        startLoopIndex=loopIndex #skip all values that has already been compared
    #print "pick value from = %s, return value = %s" % (similarvaluelist,mostFreqValue)
    return mostFreqValue

def create_similarvalue_list(words, similarWord):
    """ Given a line of words, put in a list of all the words that are 95% match in similarity """
    global numSimComp
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
        numSimComp += 1
        
        #skip all previous value.  
            #*This code is comment out because in order to get the frequent value for every value, 
            #*it has to be compared again to all previous values
        #if (myindex < index):
        #    continue;

        #apply string matching, if meet threshold, then words are the same
        matchPercentage = difflib.SequenceMatcher(None, eachValue, similarWord).ratio()
        print "match percentage between %s and %s is %f" % (eachValue, similarWord, round(matchPercentage,2))
        if (matchPercentage > .95):
            #print "adding to similar value list for word : %s" % word
            similarValueList.append(eachValue)
    return similarValueList

def isValueCompared(compareValue, alreadyCommparedValues):
    """ Returns True if the passed in word has already been compared previously """
    global numCalcDistComp
    if (len(alreadyCommparedValues) != 0):
        for eachValue in alreadyCommparedValues:
            numCalcDistComp += 1
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
        alreadyCommparedValues = Set()
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
            line_starttime=time.time()
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
                start_time = time.time()
                similarValueList = create_similarvalue_list(words, word)
                end_time = time.time()
                log_timer("create_similarvalue_list", start_time, end_time, 0)
                
                #get the most frequently appeared value
                start_time = time.time()
                attrvalue = get_frequent_value(similarValueList)
                end_time = time.time()
                log_timer("get_frequent_value", start_time, end_time, 0)

                #count the number of distinct value
                #keeps a list of all the values that has already been compared, if the next value is not in the list, then it's distinct
                start_time = time.time()
                if not isValueCompared(word, alreadyCommparedValues):
                    #if it's a new comparision value, then get the count on the number of times for that value
                    valuecountdict[attrvalue] = len(similarValueList) 
                    if not isBlankWord:
                        distinctcount += 1
                
                end_time = time.time()
                log_timer("isValueCompared", start_time, end_time, 0)

                #add value to exclusion list so it won't get compared again. 
                alreadyCommparedValues.add(attrvalue) 
                
                #add each value to the list to generate a clean list
                #print "most frequent value is %s" % attrvalue
                attrvaluelist.append(attrvalue)
                similarvaluelist = []
                isValueExcluded = False
            line_endtime=time.time()
            log_timer("Processing totalword [%s] for key [%s]" % (totalcount,key), line_starttime, line_endtime, 1)
            
            #write out for the summary
            value = "%s|%s|%s|%s|%s" % (str(distinctcount), str(blankcount), totalcount, attrvaluelist, valuecountdict)
            print "%s|%s" % (key, value)

if __name__ == "__main__":
    sys.stderr.write("---Starting mapper job---\n")
    start_time = time.time()
    start_mapper()
    #print timeit.timeit(start_mapper)
    end_time = time.time()
    log_timer("\n---------Metrics--------\n \
numSimComp_time [%s] \t numFreqValComp_time [%s] \t numCalcDistComp_time [%s]\n \
numSimComp [%s] \t\t numFreqValComp [%s] \t\t numCalcDistComp [%s]\n" % (round(createSimList_totaltime,2), round(getFreqVal_totaltime,2), round(isValComp_totaltime,2), numSimComp, numFreqValComp, numCalcDistComp), start_time, end_time, 1)
