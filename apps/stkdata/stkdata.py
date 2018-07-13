import sys, os, optparse, platform, time, re, commands, logging

from simtoolkit import data

option_parser = optparse.OptionParser(usage="%prog command STKData [STKData [STKData [...]]] [command parameter(s)]\n"+\
"STKData - a data source:\n"+\
"  File or URL\n\n"+\
"Commands:\n"+\
"  ls STKData  [record [chunk_index]]  - lists out all records or record size or data in STKData\n"+\
"                                    EXAMPLES:\n"+\
"                                     ls x.stkdata                   - shows all name in the file\n"+\
"                                     ls x.stkdata /rec/n12          - shows number of chunkes in record /rec/n12\n"+\
"                                     ls x.stkdata /rec/n12 7        - shows data in chunk 7 of record /rec/n12\n"+\
"                                     ls x.stkdata /rec/n12 (2,14,3) - shows data in chunks 2,5,8, and 12 of record /rec/n12\n"+\
"  *cp STKData  [record [chunk_index]]   STKData  \n"+\
"                                      - copy records from one data source to an other one\n"
"  -------------------------------------------------------------------------------------------------------------------------\n"+\
"  rm STKData  record [chunk_index]    - remove a whole records or only a specific chunk of data in STKData\n"
"  *defrag STKData                      - defragmentation STKData\n"
""
)
##>>Other options here
option_parser.add_option(      "--log-level",                                dest="log_level",  default="DEBUG",\
				help="Level of logging may be CRITICAL, ERROR, WARNING, INFO, or DEBUG (default DEBUG)") 
option_parser.add_option(      "--log-file",                                 dest="log_file",  default=None,\
				help="Log file name (default None)") 
option_parser.add_option("-l", "--long-output",      action="store_true",    dest="longoutout",  default=False,\
				help="print more information than just data") 
option_parser.add_option("-A", "--autocorrection",   action="store_true",    dest="autocorrect",  default=False,\
				help="Turn on autocorrection of data records (note it can restore deleted records or chunks)") 
option_parser.add_option("-D", "--autodefragmentation",  action="store_true",    dest="autodefrag",  default=False,\
				help="Turn on autodefragmentation of data records") 

options, args = option_parser.parse_args()

if options.log_file != None:
	logging.basicConfig(file=options.log_file, format='%(asctime)s:%(name)-33s%(lineno)-6d%(levelname)-8s:%(message)s', level=eval("logging."+options.log_level) )
else:
	logging.basicConfig(format='%(asctime)s:%(name)-33s%(lineno)-6d%(levelname)-8s:%(message)s', level=eval("logging."+options.log_level) )

CMD = args[0]
PRM = {}
if options.autocorrect:
	PRM["autocorrection"]=True
if options.autodefrag:
	PRM["autodefragmentation"] = True
if CMD == "ls":
	if   len(args) == 2:
		with data(args[1],**PRM) as d:
			if options.longoutout:
				print "file            :",args[1]
				print "record names    :"
			for rec in d:
				if options.longoutout:
					print "  >", rec,"[{}]".format(d[rec,])
				else:
					print rec
	elif len(args) == 3:
		with data(args[1],**PRM) as d:
			if not args[2] in d:
				sys.stderr.write("\n-----------------------\n"+\
				   "Cannot find record %s in %s \n\n"%(args[2],args[1]) )
				exit(1)	
			if options.longoutout:
				print "file            :",args[1]
				print "name            :",args[2]
				print "number of chunks:",d[args[2],]
			else:
				print d[args[2],]
	elif len(args) == 4:
		try:
			nchunk = eval(args[3])
		except BaseException as e:
			sys.stderr.write("\n-----------------------\n"+\
				   "Cannot recognize record index {}: {} \n\n".format(args[3],e ) )
			exit(1)	
		with data(args[1],**PRM) as d:
			if not args[2] in d:
				sys.stderr.write("\n-----------------------\n"+\
				   "Cannot find record %s in %s \n\n"%(args[2],args[1]) )
				exit(1)	
			if type(nchunk) is int:
				if nchunk >= d[args[2],]:
					sys.stderr.write("\n-----------------------\n"+\
					   "Requested chunk %d is out of range [0,%d] for record %s in %s \n\n"%(nchunk,d[args[2],]-1,args[2],args[1]) )
					exit(1)	
				if options.longoutout:
					print "file            :",args[1]
					print "name            :",args[2]
					print "chunk           :",args[3]
					print "data            :",d[args[2],nchunk]
				else:
					print d[args[2],nchunk]
			if type(nchunk) is tuple:
				if options.longoutout:
					print "file            :",args[1]
					print "name            :",args[2]
					print "chunk           :",args[3]
					print "data            :",d[args[2],nchunk]
				else:
					print d[args[2],nchunk]
	else:
		sys.stderr.write("\n-----------------------\n"+\
		   "Incorrect number of parameters for command 'ls' :\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
elif CMD == "rm":
	if   len(args) == 3:
		with data(args[1],**PRM) as d:
			del d[args[2]]
	elif len(args) == 4:
		nchunk = eval(args[3])
		with data(args[1],**PRM) as d:
			del d[args[2],nchunk]
	else:
		sys.stderr.write("\n-----------------------\n"+\
		   "Incorrect number of parameters for command 'rm' :\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	
