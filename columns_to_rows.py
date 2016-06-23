#!/usr/bin/env python

import sys
import csv
import logging

def usage():
    print "Usage: %s %s %s %s" % (__file__, "<cfg path>", "<csv file>", "<output of transposed csv>")
    print "EX: %s %s %s %s" % (__file__, "./config", "pie.csv", "pietranposed.csv")

try:
    sys.path.append(sys.argv[1])
    import pyconfig #user import for python code config file
except Exception as error:
    print "ERROR!!!! Missing config path"
    usage()
    raise Exception(error)

def set_logger():
    global logger
    logfile = pyconfig.logfile
    FORMAT = pyconfig.logFormat
    logging.basicConfig(filename=logfile,filemode=pyconfig.writeMode,format=FORMAT, datefmt=pyconfig.dateFormat)
    logger = logging.getLogger('root')
    logger.setLevel(pyconfig.logLevel)

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
    arg2 -- the output tranpose file
    """
   
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    global infile
    global outfile

    try:
        infile = sys.argv[2]
        outfile = sys.argv[3]
    except IndexError as error:
        logger.error("Not enough arguments %s" % error)
        raise IndexError(error)
    except Exception as detail:
        logger.error("Unexpected Error:", error)
        raise Exception(error)
    logger.info("Exit %s", sys._getframe().f_code.co_name)

def get_file_reader():
    """Get the file reader pointer"""
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    try:
        with open(infile, 'rb') as f:
            reader = csv.reader(f)
            lines = list(reader)
    except IOError as error:
        logger.error("Unable to open %s file with error %s", infile, error)
        raise IOError(error)
    except Exception as error:
        logger.debug("Unexpected Error:", error)
        raise Exception("Unexpected Error:", error)
    logger.info("Exit %s", sys._getframe().f_code.co_name)
    return lines 

def get_file_writer():
    """get the file writer pointer"""
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    try:
        filewriter = open(outfile, 'w')
    except IOError as error:
        logger.error("Unable to open %s file with error %s", outfile, error)
        raise IOError(error)
    except Exception as error:
        logger.debug("Unexpected Error:", error)
        raise Exception("Unexpected Error:", error)
    logger.info("Exit %s", sys._getframe().f_code.co_name)
    return filewriter 

def write_transposed_element(headerlist, keyvaluelist):
    """Write to file after transposed"""
    logger.info("Enter %s", sys._getframe().f_code.co_name)
    index = 0
    filewriter = get_file_writer()
    for header in headerlist:
        filewriter.write(header)
        for eachvalue in keyvaluelist[index]:
            filewriter.write("|")
            filewriter.write(eachvalue)
        filewriter.write("\n")
        index += 1
    logger.info("Exit %s", sys._getframe().f_code.co_name)

def has_required_attributes(headerlist):
    """ Check required attributes """ 
   
    if "category_nbr" in headerlist:
        if "department" in headerlist:
            if "upc" in headerlist:
                if "cdt" in headerlist:
                    logger.info("Found all necessary attributes")
                    return True
                else:
                    logger.warning("Missing product segment attribute")
                    return False
            else:
                logger.warning("Missing upc attribute")
                return False
        else:
            logger.warning("Missing department attribute")
            return False
    else:
        logger.warning("Missing category attribute")
        return False

def tranpose_file():
    """Tranpose from an csv file from columns oriented to row oriented"""
    logger.info("Enter %s", sys._getframe().f_code.co_name)

    firstline = True 
    headerlist = []
    index = 0 
    headerlength = 0
    keyvaluelist = {}
    
    lines = get_file_reader()
    for line in lines:
        #first line are the attributes name
        if (firstline):
            firstline = False
            headerlist = line

            #check for required attributes
            #if (not (has_required_attributes(headerlist))):
            #    logger.error("Not all required attributes found. Exiting...")
            #    exit(1)

            headerlength = len(headerlist)
            index = 0
            #for each header atrribute, make a new list
            for item in headerlist:
                keyvaluelist[index] = []
                index += 1
            continue  #done with parse out header from attribute names
        index = 0
        
        #each header attribute is a new list, add the each value to the correct list
        for eachvalue in line:
            #get the existing list and add another value to it
            mylist = keyvaluelist[index]
            mylist.append(eachvalue.upper())
            keyvaluelist[index] = mylist    #replace the value of that header with the new values as it gets new value
            index += 1
    
    #write out the header and it's value within the list
    write_transposed_element(headerlist, keyvaluelist)
    logger.info("Exit %s", sys._getframe().f_code.co_name)

def main():
    """Start script to tranpose csv file"""
    set_logger()
    
    logger.info("\n\n~~~~~~~~~~~~~~~~~~~~~~~START PROGRAM %s~~~~~~~~~~~~~~~~~~~~~~~~~~~~", __file__)
    try:
        get_usr_input()
    except Exception as error:
        print "Exception caught: ", error
        print "Usage: %s %s %s %s" % (__file__, "<cfg path>", "<csv file>", "<output of transposed csv>")
        print "EX: %s %s %s %s" % (__file__, "./config", "pie.csv", "pietranposed.csv")
        exit(1)

    try:
        tranpose_file()
    except Exception as error:
        print "Exception caught: ", error
        exit(1)
    logger.info("\n~~~~~~~~~~~~~~~~~~~~~~~END PROGRAM %s~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n", __file__)

#main script starts here
main()
