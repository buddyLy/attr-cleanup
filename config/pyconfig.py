import logging

#logdir="/u/users/$USER/attr-cleanup/logs/"
logdir="./logs/"
pylogfile="pylog.log"
logfile=''.join([logdir, pylogfile])
#logLevel=logging.DEBUG
logLevel=logging.INFO
logFormat="[%(asctime)s:%(filename)s:%(levelname)s:%(funcName)10s():%(lineno)d] %(message)s"
appendMode="a"
overwriteMode="w"
dateFormat="%m/%d/%Y %I:%M:%S %p"
writeMode=appendMode

#these are the attributes not to be replaced with the generatic name 'attributeN'
retail_channel_code="retail_channel_code"
category="category"
upc="upc"
segment="segment"
dept="dept"
