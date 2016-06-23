#!/usr/bin/env python

import sys
import csv
import datetime
import time
import os
import logging
import sys

def usage():
        print "Usage: %s %s %s %s %s %s %s" % (__file__, "<cfg path>", "<cleansed csv file>", "<output of map file>", "<output of header file>", "<dept nbr>", "<retail channel code>")
        print "EX: %s %s %s %s %s %s %s" % (__file__, "./config", "cleansed_attr.csv", "mapfile.txt", "headerfile.txt", "4324", "1")
try:
    sys.path.append(sys.argv[1])
    import pyconfig #user import for python code config file
except Exception as error: 
    print "ERROR!!!! Missing config path" 
    usage()
    raise Exception(error) 

def set_logger():
    global logger
    logger = logging.getLogger('root')
    logger.setLevel(pyconfig.logLevel) 

    logfile = pyconfig.logfile
    FORMAT = pyconfig.logFormat 
    logging.basicConfig(filename=logfile,filemode=pyconfig.writeMode,format=FORMAT, datefmt=pyconfig.dateFormat)

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

def get_usr_input():
    """ 
    Get input from user command line 
    arg1 -- file to read from
    arg2 -- result file
    arg3 -- department nbr
    arg4 -- retail channel code
    arg5 -- header line with attributes name replaced with generic name
    """
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    global infile
    global mapfile
    global headerfile
    global department
    global retail_channel_code

    try:
        #cfgfile = sys.argv[1]
        infile = sys.argv[2]
        mapfile = sys.argv[3]
        headerfile = sys.argv[4]
        department = sys.argv[5]
        retail_channel_code = sys.argv[6]
    except IndexError as error:
        logger.error("Not enough arguments %s" % error)
        raise IndexError(error)
    except Exception as detail:
        logger.error("Unexpected Error:", error)
        raise Exception(error)
    logger.info("Exit %s", sys._getframe().f_code.co_name)

def write_header_line(headerLine):
    """
    Write the header line, which will be used to create the attribute cleanup table
    """
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    try:
        filewriter = open(headerfile, 'w')
        #filewriter.write(headerLine)
    except IOError as error:
        logger.error("Unable to open [%s] file with error [%s]", headerfile, error)
        raise IOError(error)
    except Exception as error:
        logger.error("Unexpected Error:", error)
        raise Exception(error)

    filewriter.write(headerLine)
    logger.info("Exit %s", sys._getframe().f_code.co_name)
    #Todo: does file need to be closed

def write_mapper_file(retail_channel_code, department, category, mytimestamp, attr_key_value, attr_column_list, attr_value_list):
    """"
    Write out the mapper line, which will be used to insert into the hive mapper table
    """
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    try:
        filewriter = open(mapfile, 'w')
    except IOError as error:
        logger.error("Unable to open %s file with error %s", headerfile, error)
        raise IOError(error)
    except Exception as error:
        logger.debug("Unexpected Error:", error)
        raise Exception("Unexpected Error:", error)

    filewriter.write(retail_channel_code)
    filewriter.write("|")
    filewriter.write(department)
    filewriter.write("|")
    filewriter.write(category) 
    filewriter.write("|")
    filewriter.write(mytimestamp) 
    filewriter.write("|")
    filewriter.write(attr_key_value)
    filewriter.write("|")
    filewriter.write(attr_column_list)
    filewriter.write("|")
    filewriter.write(attr_value_list)
    #Todo: does file need to be closed
    logger.info("Exit %s", sys._getframe().f_code.co_name)

def skip_attribute(item):
    """
    Skip the attributes that will not be converted to a generic attribute.
    Returns True if constant attribute found
    Returns False for regular attributes
    """
    try:
        if pyconfig.retail_channel_code in item:
            return True
        if pyconfig.category in item:
            return True
        if pyconfig.upc in item:
            return True
        if pyconfig.segment in item:
            return True
        if pyconfig.dept in item:
            return True
    except Exception as error:
        logger.error("Error skipping generic attributes with error %s", error)
        raise Exception(error)
    return False

def get_header_info():
    """
    Read cleansed file and parsed out the header line to retrieve generate the generic attribute and extract category number
    """
    logger.info("Enter %s", sys._getframe().f_code.co_name) 
    try:
        with open(infile, 'rb') as f:
            reader = csv.reader(f)
            lines = list(reader)
    except Exception as error:
        logger.error("Error opening file %s with error %s", infile, error)
        raise Exception(error)
   
    #Retrieve time stamp in format: YYYYMMDD_HHMMSS
    ts = time.time()
    mytimestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')

    index = 0
    maxread = 2
    category = ""
    categoryIndex = 0
    attr_column_list = ""
    attr_value_list = ""
    attr_key_value = ""
    header = ""
    for line in lines:
        colIndex = 1
       
        #Once we get the category number from cleansed file, no need to read the rest of the file
        if (index == maxread):
            break

        #Get the header line and parsed out the attributes and values
        if (index == 0):
            logger.debug("First line: %s", line)
            for item in line:
                logger.debug("item at %d is %s", colIndex, item)
                #Since each header attribute value has a data type, strip out the data type
                try:
                    attrvalue = item.split(" ")[0]
                    attrtype = item.split(" ")[1]
                except Exception as error:
                    logger.debug("Unable to get header attribute with error: [%s]", error)
                    raise Exception(error)
                #Get the position of the category number to retrieve the category number at that position
                if "category_nbr" in item:
                    logger.debug("Found category at position %d with item named %s", colIndex, item)
                    #store the positon of the category to retrieve on the next line
                    categoryIndex = colIndex

                if (skip_attribute(item)):
                    #capture the attribute before skipping, separating each attribute with a comma
                    header = "".join([header, item, ","])
                    colIndex += 1
                    continue
               
                #the generic name for an attribute will start with "attribute" follow by an increasing index number 
                columnName = "attribute" + str(colIndex)
                header = "".join([header, columnName, " ", attrtype, ","])
                logger.debug("Current header is [%s]", header)
                attr_key_value = attr_key_value + columnName + ":" + attrvalue + "~"
                attr_column_list = "".join([attr_column_list, str(columnName), ","])
                attr_value_list = "".join([attr_value_list, str(attrvalue), ","])
                colIndex += 1
            #write out the header stripping out the last comma
            write_header_line(header[:-1])
        if (index == 1):
            #retrieve category after knowing the position of the category
            category = line[categoryIndex-1]
        #move on the next line in the file
        index += 1
    write_mapper_file(retail_channel_code, department, category, mytimestamp, attr_key_value[:-1], attr_column_list[:-1], attr_value_list[:-1])
    logger.info("Exit %s", sys._getframe().f_code.co_name)

def main():
    """Start script to generate the mapper file"""
    set_logger()
    
    logger.info("\n\n~~~~~~~~~~~~~~~~~~~~~~~START PROGRAM %s~~~~~~~~~~~~~~~~~~~~~~~~~~~~", __file__)

    try:
        get_usr_input()
    except Exception as error:
        print "Exception caught: ", error 
        usage()
        exit(1)

    try:
        get_header_info()
    except Exception as error:
        print "Exception caught: ", error 
        exit(1)

    logger.info("\n~~~~~~~~~~~~~~~~~~~~~~~END PROGRAM %s~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", __file__)

#main script starts here
main()
