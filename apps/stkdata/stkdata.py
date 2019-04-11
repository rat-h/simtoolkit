import sys, os, optparse, platform, time, re, commands, logging

from simtoolkit import data

option_parser = optparse.OptionParser(usage="%prog command STKData [STKData [STKData [...]]] [command parameter(s)]\n"+\
"""
STKData - a data source:
  File or URL\n
Commands:
  ls STKData  [record [chunk_index]]       - lists out all records or record size or data in STKData
                                    EXAMPLES:
                                     ls x.stkdata                   - shows all name in the file
                                     ls x.stkdata /rec/n12          - shows number of chunkes in record /rec/n12
                                     ls x.stkdata /rec/n12 7        - shows data in chunk 7 of record /rec/n12
                                     ls x.stkdata /rec/n12 (2,14,3) - shows data in chunks 2,5,8, and 12 of record /rec/n12
  TODO> cp STKData  [record [chunk_index]]   STKData  [record [chunk_index]]
  TODO>                                    - copy records from one data source to an other one
  TODO> ln STKData  [record [chunk_index]]   STKData  [record [chunk_index]]
  TODO>                                    - create a link of the data to an aggregating file
  TODO> cat STKData [STKData [STKData [...]]] -o STKData
  TODO>                                    - concatenate all data in one -o data storage
  TODO> mv  STKData [STKData [STKData [...]]] -o STKData
  TODO>                                    - move all data in one -o storage, if STKData are files, they will be removed
  -------------------------------------------------------------------------------------------------------------------------
  rm STKData  record [chunk_index]         - remove a whole records or only a specific chunk of data in STKData
  defrag STKData                           - defragmentation of STKData
  -------------------------------------------------------------------------------------------------------------------------
  db get STKData [record [chunk_index]]    - get all or record or only one chunk data
  TODO>db set STKData record [chunk_index] [new_record_position_size_or_type]
  TODO>                                    - set all record or only one chunk data
"""
)
##>>Other options here
option_parser.add_option("-o", "--output",                                   dest="output",  default=None,\
				help="The output for concatenate and move commands") 
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

PRM["autocorrection"]      = options.autocorrect
PRM["autodefragmentation"] = options.autodefrag

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
	
elif CMD == "defrag":
	if   len(args) == 2:
		with data(args[1],**PRM) as d:
			d.defragmentation()
	else:
		sys.stderr.write("\n-----------------------\n"+\
		   "Incorrect number of parameters for command 'defrag' :\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
elif CMD == "db":
	#debug mode
	CMD=args[1]
	args = args[2:]
	if CMD == "get":
		if   len(args) == 1:
			with data(args[0], **PRM) as d:
				for name in  d:
					for fd,st,sz,tp in d.datamap[name]:
						print name,":",fd,st,sz,tp
		elif len(args) == 2:
			with data(args[0], **PRM) as d:
				if not args[1] in d.datamap:
					sys.stderr.write("\n-----------------------\n"+\
						"Cannot find record %s in %s \n\n"%(args[1],args[0]) )
					exit(1)
				for fd,st,sz,tp in d.datamap[args[1]]:
					print args[1],":",fd,st,sz,tp
			
		elif len(args) == 3:
			ch = int(args[2])
			with data(args[0], **PRM) as d:
				if not args[1] in d.datamap:
					sys.stderr.write("\n-----------------------\n"+\
						"Cannot find record %s in %s \n\n"%(args[1],args[0]) )
					exit(1)
				if len(d.datamap[args[1]]) <= abs(ch) : 
					sys.stderr.write("\n-----------------------\n"+\
						"There are only %d chunks in record %s of %s \n\n"%(len(d.datamap[args[1]]), args[1],args[0]) )
					exit(1)
				fd,st,sz,tp = d.datamap[args[1]][ch]
				print "{}[{}]".format(args[1],ch),":",fd,st,sz,tp
		else:
			sys.stderr.write("\n-----------------------\n"+\
		   "Incorrect number of parameters for command 'db get' :\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
			exit(1)
			
