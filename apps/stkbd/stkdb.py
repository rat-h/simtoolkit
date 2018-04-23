#! /usr/bin/env python

import sys, os, optparse, platform, time, re, commands, logging

from simtoolkit import db, tree
from datetime import datetime
from random import randint
import sqlite3

option_parser = optparse.OptionParser(usage="%prog database command [command parameter(s)]\n\nDatabase:\n  File or URL\n\nCommands:\n"+\
"  ls           - list all records or parameters with filter name if record is given\n"+\
"                 EXAMPLES:\n"+\
"                  ls                                                 - shows all records in database (hash, time-stamp, message)\n" +\
"                  ls  7945*                                          - shows all records which hash start on 7945 \n"+\
"                  ls  79455f9cc5cfda64bf77f59bb60113a4492ab3f8       - shows record(s) with hash 79455f9cc5cfda64bf.....\n"+\
"                  ls  79455f9cc5cfda64bf77f59bb60113a4492ab3f8 -t    - as before but now it will print out full tree of parameters\n"+\
"                                                                        for each record without \n"+\
"                  ls  7945* /                                        - prints out all parameters in recordings starts with 7945\n"+\
"                  ls  7945* /Connections/                            - prints out /Connections parameters in recordings starts with 7945\n"+\
"                  ls  7945* /Connections/*/gmax-*                    - prints out /Connections/*/gmax-* parameters in recordings starts with 7945\n"+\
"                  ls  -n                                             - shows list of names\n"+\
"  ------------------------------\n"+\
"  create HASH MESSAGE \n"+\
"               - creates new empty set of parameters with the HASHSUM and message\n"+\
"                 EXAMPLE:\n"+\
"                  create bd5f80e8be44c0965bab82698b24a6b6e4283816 \'blach-blach-blach\' \n"+\
"  set KEY KEY VALUE \n"+\
"               - sets in simulation(s) selected by the first key, parameter(s) selected by the second key into value\n"+\
"  get KEY [KEY]- get a value of parameter in parameter defined by second key in record defined by the first key \n"+\
"  rm  KEY [KEY]- remove record or parameter(s) from record\n"+\
"                 EXAMPLE:\n"+\
"                  rm 79455f9cc5cfda64bf77f59bb60113a4492ab3f8        - remove hole record 79455f9cc5cfda64bf.....\n"+\
"                  rm 79455*                                          - remove all records start with 79455\n"+\
"                  rm 79455*  /Connections/EE/                        - remove all /Connections/EE/ parameters in records start with 79455\n"+\
"                  rm 79455*  /Connections/*/gmax-*                   - remove all /Connections/*/gmax-* parameters in records start with 79455\n"+\
"  ------------------------------\n"+\
"  message KEY [new message] \n"+\
"               - edit message in record marked by KEY\n"+\
"                 if new message isn't provided, reads /STKDB/editor or /GENERAL/editor in configuration file\n"+\
"  ------------------------------\n"+\
"  tag  COMMAND - operations with tag\n"+\
"       ls               - shows all tags\n"+\
"       ls TAG [-t][-l]  - shows parameters in simulation with TAG\n"+\
"                         EXAMPLE:\n"+\
"                          tag ls  v.01.*\n"+\
"       set TAG KEY      - sets tag TAG for record(s) selected by KEY\n"+\
"                         EXAMPLE:\n"+\
"                           tag set v.01.fast 79455f9cc5cfda64bf77f59bb60113a4492ab3f8 \n" +\
"                           tag set v.01.slow 2017-11-21/11:30:15.47 \n" +\
"                           tag set v.01.ALL  2017-11-21/*          - will mark all recordings made this day by v.01.ALL\n" +\
"       rm TAG           - remove tag\n"+\
"  ------------------------------\n"+\
"  info        - print out information about data base\n"+\
"  db recs|names|values [fliter [column]]\n"+\
"              - debug information. see simtoolkit.database for more information\n"+\
"\n===================================")
option_parser.add_option("-n", "--names",            action="store_true",    dest="names",      default=False,\
				help="Show list of names for all possible parameters in data base") 
option_parser.add_option("-t", "--tree",             action="store_true",    dest="tree",       default=False,\
				help="Print out parameters as a tree, not a table") 
option_parser.add_option("-l", "--list-parameters",  action="store_true",    dest="plist",      default=False,\
				help="Print out parameters as a tree, not a table") 
option_parser.add_option(      "--print-full",       action="store_true",    dest="printfull",  default=False,\
				help="Shows all parameters for every simulation record") 
option_parser.add_option(      "--log-level",                                dest="log_level",  default="DEBUG",\
				help="Level of logging may be CRITICAL, ERROR, WARNING, INFO, or DEBUG (default DEBUG)") 
option_parser.add_option(      "--log-file",                                 dest="log_file",  default=None,\
				help="Log file name (default None)") 
options, args = option_parser.parse_args()

if options.log_file != None:
	logging.basicConfig(file=options.log_file, format='%(asctime)s:%(name)-33s%(lineno)-6d%(levelname)-8s:%(message)s', level=eval("logging."+options.log_level) )
else:
	logging.basicConfig(format='%(asctime)s:%(name)-33s%(lineno)-6d%(levelname)-8s:%(message)s', level=eval("logging."+options.log_level) )

if len(args) < 2:
	sys.stderr.write("\n-----------------------\n"+\
	                   "Needs more parameters:\n"+\
	                   "use {} -h for more help\n\n".format(sys.argv[0]))
	exit(1)
#if 
	#sys.stderr.write("----------------------------------------------------\n")
	#sys.stderr.write("STKDB: DATABASE ERROR\n")
	#sys.stderr.write("     : Cannot open data base {} : {}\n".format(args[0],e))
	#sys.stderr.write("----------------------------------------------------\n")		
	#exit(1)



def printhead(h,t,m,g=None,idx=None):
	if not idx is None:
		print "INDEX   :",idx
	print "HASH    :",h
	print "TIME    :",t.replace(" ","/")
	print "MESSAGE :",m.strip(" \n\t\a").replace("\n","\n               ")		
	if not g is None:
		print "TAG        :",g
def printtree(h,t,m,Atree,g=None,idx=None):
	print "\n================================================================="
	printhead(h,t,m,g=g,idx=idx)
	print "TREE    :/"
	for p,k in Atree.printnames():
		if k is None:
			print "        ",p
		else:
			if len(Atree[k]) > 100 and not options.printfull:
				print "        ",p,Atree[k][:35]," ... ",Atree[k][-35:]
			else:
				print "        ",p,"{}".format(Atree[k]).replace("\n", " ")
	print
def getpytype(v):
	try:
		return int(v)
	except:
		return v
		
stkdb = db(args[0])
CMD = args[1]
if CMD == "ls":
	if options.names:
		for n, in stkdb.poolnames(None if len(args) <3 else args[2]):
			print "{}".format(n)
	elif len(args) <3 :
		for h,t,m in stkdb:
			print "\n================================================================="
			printhead(h,t,m)
	elif len(args) < 4:
		for h,t,m in stkdb.poolrecs(getpytype(args[2].replace("/"," "))):
			print "\n================================================================="
			printhead(h,t,m)
	else:
		ch,ct = '',''
		if options.tree: Atree = tree()
		for h,t,m,n,v in stkdb.pool(getpytype(args[2].replace("/"," ")),getpytype(args[3])):
			if ch != h or ct != t:
				if ch !="" and ct != "":
					if options.tree:
						printtree(h,t,m,Atree)
						Atree = tree()
					else: printhead(h,t,m) 
				else:
					if not options.tree:printhead(h,t,m) 
				ch,ct = h,t
			if options.tree: 
				Atree[n] = v
			else:
				if len(v) > 100  and not options.printfull:
					print "  ",n,v[:35]," ... ",v[-35:]
				else:
					print "  ",n,"{}".format(v).replace("\n"," ")
		else:
			if ch !="" and ct != "":
				if options.tree:
					printtree(h,t,m,Atree)
elif CMD == "create":
	if len(args) <  4:
		sys.stderr.write("\n-----------------------\n"+\
		   "Needs more parameters:\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	elif len(args) <  5:
		now = datetime.now()
		timestamp = "%d-%d-%d %d:%d:%d.%d"%(now.year, now.month, now.day, now.hour, now.minute, now.second, randint(0,999))
	else :
		timestamp = args[4]
	stkdb.db.mkrec(timestamp,args[2],args[3])
elif CMD == "set":
	if len(args) <  5:
		sys.stderr.write("\n-----------------------\n"+\
		   "Needs more parameters:\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	rec = getpytype(args[2])
	par = getpytype(args[3])
	var = getpytype(args[4])
	stkdb[rec,par] = var
elif CMD == "get":
	if  len(args) <  3:
		sys.stderr.write("\n-----------------------\n"+\
		   "Needs more parameters:\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	flt = getpytype(args[2])
	recs = [ (i,h,t,m) for i,h,t,m in stkdb.recs(flt=flt) ]
	if len(recs) == 0:
		sys.stderr.write("stkdb: Cannot find record which satisfy to {}\n".format(flt))
		exit(1)
	if len(recs) > 1:
		sys.stderr.write("stkdb: Find more that one record which satisfy to {}\n".format(flt))
		for i,h,t,m in recs:
			print "\n================================================================="
			printhead(h,t,m,idx=i)
		exit(1)
	i,hs,ts,ms=recs[0]	
	if  len(args) <  4:
		Atree = stkdb[i]
	else:
		Atree = stkdb[i ,getpytype(args[3])]
	printtree(hs,ts,ms,Atree,idx=i)
elif CMD == "rm":
	print "Doesn't work yet -.-"
	exit(1)
elif CMD == "message":
	if len(args) < 3 :
		sys.stderr.write("\n-----------------------\n"+\
		   "Needs more parameters:\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	key = getpytype(args[2])
	if len(args) > 3 :
		message = args[3]
	else:
		from simtoolkit import __config__
		if   "/STKDB/editor" in __config__.stk_config:
			editor = __config__.stk_config["/STKDB/editor"]
		elif "/GENERAL/editor" in __config__.stk_config:
			editor = __config__.stk_config["/GENERAL/editor"]
		else:
			sys.stderr.write("\n-----------------------\n"+\
			   "Cannot find editor. Please set it:\n"+\
			   "stk-config STKDB/editor=vi \n")
			exit(1)
			
		for i,h,m in stkdb.getmessage(key):
			mfile = h+".msg"
			with open(mfile,"w") as fd: fd.write(m)
			pre_nstp = os.stat(mfile).st_mtime
			os.system(editor+" "+mfile)
			pos_nstp = os.stat(mfile).st_mtime
			if pre_nstp == pos_nstp:
				sys.stderr.write("\n-----------------------\n"+\
				"message {} wasn't changed\n".format(h))
				os.remove(mfile)
				continue
			fsize = os.path.getsize(mfile)
			with open(mfile) as fd:
				message = fd.read(fsize)
			os.remove(mfile)
			stkdb.setmessage(i,message)

			
	
elif CMD == "tag":
	if len(args) < 3 :
		sys.stderr.write("\n-----------------------\n"+\
		   "Needs more parameters:\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	CMD = args[2]
	if   CMD == "set":
		if len(args) < 5 :
			sys.stderr.write("\n-----------------------\n"+\
			   "Needs more parameters:\n"+\
			   "use {} -h for more help\n\n".format(sys.argv[0]))
			exit(1)
		stkdb.settag(args[4],args[3])
	elif CMD == "ls":
		for tag,recid,timestamp,rechash,message in stkdb.pooltags(key = args[3] if len(args) >= 4 else None):
			if options.tree:
				printtree (rechash,timestamp,message,stkdb[recid],g=tag)
			else:
				print "\n================================================================="
				printhead(rechash,timestamp,message,g=tag)
				if   options.plist:
					for h,t,m,n,v in stkdb.pool(recid,args[3] if len(args) < 4 else "/"):
						if len(v) > 100  and not options.printfull:
							print "  ",n,v[:35]," ... ",v[-35:]
						else:
							print "  ",n,"{}".format(v).replace("\n"," ")
	elif CMD == "rm" :
		if len(args) < 4 :
			sys.stderr.write("\n-----------------------\n"+\
			   "Needs more parameters:\n"+\
			   "use {} -h for more help\n\n".format(sys.argv[0]))
			exit(1)
		stkdb.rmtag(args[3])
	else:
		sys.stderr.write("\n-----------------------\n"+\
		   "Unknown \'{}\' command for tag command\n".format(CMD)+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
elif CMD == "db":
	if len(args) < 3:
		sys.stderr.write("\n-----------------------\n"+\
		   "Needs more parameters:\n"+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
	CMD    = args[2]
	flt    = getpytype(args[3]) if len(args) >= 4 else None
	column = getpytype(args[4]) if len(args) >= 5 else None
	if   CMD == "recs":
		for i,h,t,m in stkdb.recs(flt=flt,column=column):
			print i,h,t,m.replace("\n"," ")
	elif CMD == "names":
		for i,n in stkdb.names(flt=flt):
			print i,n
	elif CMD == "values":
		for i,r,n,v in stkdb.values(flt=flt,column=column):
			if len(v) > 100  and not options.printfull:
				print i,r,n,v[:35]," ... ",v[-35:]
			else:
				print i,r,n,"{}".format(v).replace("\n"," ")
	else:
		sys.stderr.write("\n-----------------------\n"+\
		   "unknown debugger command: {} \n".format(CMD)+\
		   "use {} -h for more help\n\n".format(sys.argv[0]))
		exit(1)
		
elif CMD == "info":
	info = stkdb.info()
	print "\n==================================== "
	print "DB INFORMATION in URLI :",args[0]
	for n in sorted(info):
		print "% 22s :"%n,info[n]
	print
else:
	sys.stderr.write("\n-----------------------\n"+\
	   "Unknown command \'{}\'\n".format(CMD)+\
	   "use {} -h for more help\n\n".format(sys.argv[0]))
	exit(1)
		
		
		
