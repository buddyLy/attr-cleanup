#!/usr/bin/env python

import sys
import difflib

def getmostfrequentvalue(similarvaluelist):
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

def start_string_match():
    # input comes from STDIN (standard input)
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        lines = line.split("\n")

        key = ""
        value = ""
        exclusionlist = []
        similarvaluelist = []
        attrvaluelist = []
        exclusionIndex = 0
        matchpercentage = 0
        blankcount = 0
        distinctcount = 0
        totalcount = 0
        firstword = 0
        valuecountdict = {}
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
                
                #if value is unique, then assigned that value to attrvalue
                #attrvalue = word

                if (word == ""):
                    #print "found blank word, increasing blank count"
                    isBlankWord = True
                    blankcount += 1
                #else:
                if (1):
                    #count of all attribute values not blank
                    totalcount += 1
                    if (len(exclusionlist) == 0):
                        exclusionlist.append(word)
                        if (isBlankWord == False):
                            distinctcount += 1
                    else:
                        for excludeword in exclusionlist:
                            #print "comparing %s to exclusion word %s. exclusionlist=%s" % (word, excludeword, exclusionlist)
                            matchpercentage = difflib.SequenceMatcher(None, excludeword, word).ratio()
                            #print "match percentage between %s and %s is %f" % (word, excludeword, round(matchpercentage,2))
                            if (matchpercentage > .95):
                                #attrvalue = excludeword
                                isValueExcluded = True
                                #print "match percentage is %s, breaking out" % matchpercentage
                                break
                        else:   #this gets executed upon exhausting of for loop, but not from break statement
                            #print "end of list. adding word %s to list and increasing distinct count" % word
                            #exclusionlist.append(word)
                            if (isBlankWord == False):
                                distinctcount += 1
                
                #if value has already been compared in similarity value list, skip to the next value
                if (1):
                #if (isValueExcluded == False):
                    myindex = 0
                    #create similar value list to get most frequent appeared value
                    for word2 in words:
                        #skip the first column because it is a key
                        if (firstword2 == 0):
                            #print "first word: %s. Skipping loop" % word2
                            firstword2 = 1
                            continue

                        myindex += 1
                        #skip all previous value
                        #if (myindex < index):
                        #    continue;

                        #apply string matching, if meet threshold, then words are the same
                        matchpercentage = difflib.SequenceMatcher(None, word2, word).ratio()
                        #print "match percentage between %s and %s is %f" % (word, word2, round(matchpercentage,2))
                        if (matchpercentage > .95):
                            #print "adding to similar value list for word : %s" % word
                            similarvaluelist.append(word2)

                    #get the most frequently appeared value
                    attrvalue = getmostfrequentvalue(similarvaluelist)
                    exclusionlist.append(word)
                    #print "compute most frequent value is %s" % attrvalue
                    #print "total count for attr=%s is %s" % (key,totalcount)
                    #print "similarvalue is : %s" % similarvaluelist
                    #if value has already been accounted for previously, then don't count it again
                    if (isValueExcluded == False):
                        valuecountdict[attrvalue] = len(similarvaluelist)
                #add each value to the list to generate a clean list
                #print "most frequent value is %s" % attrvalue
                attrvaluelist.append(attrvalue)
                similarvaluelist = []
                isValueExcluded = False
            #write out for the summary
            value = "%s|%s|%s|%s|%s" % (str(distinctcount),str(blankcount), totalcount, attrvaluelist, valuecountdict)
            print "%s|%s" % (key, value)

if __name__ == "__main__":
        start_string_match()

