import os, sys, types, logging, re, hashlib
from collections import OrderedDict
from simtoolkit.tree     import tree
from simtoolkit.database import db
from numpy import *



class methods:
	"""
	The class \'methods\' contains all aprameters and sometimes some results of a model.
	This class seems as a tree of objects, so parameter can be called by name.
	EXAMPLE:      methods["/parameter/a"]
	
	The method class parses configuration file(s) and command lines arguments to create a 
	  text tree methods_txt.
	The text tree contanes parameter:string_representation_of_the_value and is used for holding 
	  textual values of parameters before generation of python objects. 
	
	To generate python object for given name read the parameter value or call
	generate(parameter_name) or call generate() without parameters for building all parameters.
	NOTE generate(parameter_name) will generate all dependencies required to make the requested parameter.
	     If, for example, /p=@/k@*2 and /k=exp(@/x@) and /x=pi, both /x and /k would be generated by call
	     generate("/p") 
	"""
	is_lambda         = lambda self, value    : isinstance(value, types.LambdaType) and value.__name__ == '<lambda>'
	__check_pattern__ = lambda self, key, pat : key[ :len(pat)] == pat
	__kcehc_pattern__ = lambda self, key, pat : key[-len(pat):] == pat
	def __init__(self,
		confile, target,  localcontext,
		argvs=None,
		dbsymbol  = "db=",   dbsepcartor = ":",
		cfsymbol  = "conf=", cfseparator = ":",
		hsymbol   = "/",     vseparator  = "=",
		refsymbol = "@",     strefsymbol = "$", refhash = "#", 
		mlvsymbol = "\\",    mseparator  = ";",
		groupopen = "{", groupend="}",
		isymbol="`",
		mmopen ="/{", mmend="}",
		pure=False):
		"""
		creates the object of methods. __init__ needs 3  parameters and accepts lots of options
		@param confile     - a name of configuration file, which methods parses to extract
		                     default parameter set (may be a list of names or open files).
		@param target      - name of methods object which will be created by this constructor
		@param localcontext- local namespace where all exec(s) should be run.
		                     can be alter by localcontext parameter in generate function
		                     if None, methods uses current context from globals()
		---
		@opt   argvs       - command line arguments, which will be processed after defaults.
		@opt   dbsymbol    - (a set of) symbol(s) to indicate that some parameters should be read
						     from data base 
		@opt   dbsepcartor - a separator to segregate database name/url, record hash/time-stamp and
		                     parameter name(s) 
		                     EXAMPLES:
						       url access, extracts record using time-stamp:
		                          db=mysql:myneuron.org:2016-03-02/*):/Populations/E:/Population/P:/Connections
		                       local STKDB file access, extracts record using hash:
		                          db=simulations.stkdb:2a077d01a8a1018c6902b20b8bcfe1e90082b952:/Populations/AN:/Populations/SBC:/Populations/GBC
		@opt   cfsymbol    - (a set of) symbol(s) to indicate that some parameters should be read
						     from additional configuration file 
		@opt   cfseparator - a separator to segregate configuration filename  and
		                     parameter name(s) 
		                     EXAMPLE:
		                       reads all parameters from an additional configuration file
		                          conf=additional.conf
		                       reads some parameters from an additional configuration file
		                          conf=additional.conf:/Populations/AN:/Populations/SBC:/Populations/GBC
		---

		@opt   vseparator  - a symbol to separate a parameter name from a value 
		@opt   hsymbol     - a symbol to present hierarchical position within a parameter name 
		@opt   refsymbol   - a symbol to indicate a referenced name 
		@opt   strefsymbol - a symbol to indicate a referenced name if the value should be converted back to a string 
		@opt   refhash     - a symbol to indicate a referenced name if the value should be a hash sum of the content 
		@opt   mlvsymbol   - a symbol to indicate continuation of value line 
		@opt   mseparator  - a symbol to separate a parameter=value couple and a message
		@opt   groupopen   - a symbol at the beginning of group subtree 
		@opt   groupend    - a symbol at the end       of group subtree 
		@opt   isymbol     - a symbol at the beginning and end  of iterator in both a name and a value 
		@opt   mmopen      - a (set of) symbol(s) at the beginning of main message, i.e. model title 
		@opt   mmend       - a (set of) symbol(s) at the end       of main message, i.e. model title 
		@opt   pure        - if True parameters with wrong format will raise an exception .

		"""
		self.logger = logging.getLogger("simtoolkit.methods")
		#--
		self.dtarget     = target
		self.dlocalctx   = localcontext
		#---
		self.dbsymbol    = dbsymbol
		self.dbsepcartor = dbsepcartor
		self.cfsymbol    = cfsymbol
		self.cfseparator = cfseparator
		#---
		self.vseparator  = vseparator
		self.hsymbol     = hsymbol
		self.refsymbol   = refsymbol
		self.strefsymbol = strefsymbol
		self.refhash     = refhash
		self.mseparator  = mseparator
		self.mlvsymbol   = mlvsymbol
		self.groupopen   = groupopen
		self.groupend    = groupend
		self.isymbol     = isymbol
		self.mmopen      = mmopen
		self.mmend       = mmend
		self.pure        = pure
		#---
		self.methods     = tree(hsymbol=self.hsymbol)
		self.methods_txt = tree(hsymbol=self.hsymbol)
		self.hashspace   = tree(hsymbol=self.hsymbol)
		self.iterate     = OrderedDict()
		self.stackcnt    = 0
		self.iterate_res = False
		#---
		self.mainmessage = ""
		if type(confile) is str or type(confile) is file:
			confile = [confile]
		elif type(confile) is tuple or type(confile) is list:
			pass
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in __init__)")
			self.logger.error(" Confile should be a string or a file object or list/tuple of strings or file objects. {} is given".format(type(confile)))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise TypeError("Confile should be a string or a file object or list/tuple of strings or file objects. {} is given".format(type(confile)))
		for cf in confile:
			self.__confreader__(cf)
		#---
		if argvs is None: return
		if type(argvs) is str:
			argvs = [ argvs ]
		elif type(argvs) is tuple or type(argvs) is list:
			if reduce(lambda x,y: x and type(y) is str, argvs, True):	pass
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __init__)")
				self.logger.error(" Not all arguments {} are strings".format(argvs))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise TypeError("Not all arguments {} are strings".format(argvs))
				return 
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in __init__)")
			self.logger.error(" Arguments should be a string or list/tuple of strings or file objects. {} is given".format(type(argvs)))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise TypeError("Arguments should be a string or list/tuple of strings or file objects. {} is given".format(type(argvs)))
		#for arg in argvs:
			
		
	# Functions to read parameters from  default configuration, stkdb and so on	
	def __confreader__(self, confile):
		"""
		reads configuration file(s) and creates text tree.
		it resolves groups and prepare iterators for farther use
		"""
		def resolve_expression(expr, sep = self.vseparator):			
			return [ x.strip(" \n\t\r") for x in expr.strip(" \n\t\r").split(sep,1) ]
		def resolve_iterators(name, sep=self.isymbol):
			copir = name.strip(" \n\t\r").split(sep)
			result = ""
			for prefix,var in map(None, copir[::2], copir[1::2]):
				if prefix is not None: result += prefix
				if var is None: continue
				parts = resolve_expression(var)
				if len(parts) > 1:
					arg,value = parts
					arg,value = arg,value
					self.iterate[sep+arg+sep]=value
				result += sep+parts[0]+sep
			return result
		
		if type(confile) is tuple or type(confile) is list:
			for cfl in  confile:
				self.__confreader__(cfl)
			return
		
		if not ( type(confile) is str or type(confile) is file ) :
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in __confreader__)")
			self.logger.error(" Confile should be a string or a file object. {} is given".format(type(confile)))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise ValueError("Confile should be a string or a file object or list of strings or open file objects. {} is given".format(type(confile)))
		if type(confile) is str:
			self.logger.debug( "reading configure file %s"%(confile))
			try:
				confile = open(confile)
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __confreader__)")
				self.logger.error(" Cannot open configuration file {}: {}".format(confile,e))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise ValueError("Cannot read configuration file {}: {}".format(confile,e))
				return
			
		message_on = False
		groups = []
		command, message, continue_on = "", "", False
		#Step one read parameters tree from the file
		for nl,line in enumerate(confile.readlines()):
			l = line.strip("\n\r")
			if len(l)             == 0               : continue
			if l.lstrip(" \t")[0] == self.mseparator : continue
			#Extracting the main message 
			if self.__check_pattern__(l,self.mmopen):
				message_on        = True
				self.mainmessage += l[len(self.mmopen):]+"\n"
				continue
			elif message_on and self.__kcehc_pattern__(l,self.mmend):
				self.mainmessage += l[:-len(self.mmend)]+"\n"
				message_on        = False
				continue
			elif message_on:
				self.mainmessage += l+"\n"
				continue
			#Parsing the rest
			l = line.strip(" \t")
			vm  = l.split(self.mseparator,1)
			if len(vm) == 1:
				vm = vm[0].strip(" \n\t\r"+self.mlvsymbol)
				if self.__kcehc_pattern__(vm,self.groupopen):
					#resolve ierators in a group
					gname = resolve_iterators(vm[:-len(self.groupopen)].strip(" \n\t\r"+self.hsymbol)) 
					groups.append(gname)
					continue
				elif self.__kcehc_pattern__(vm,self.groupend):
					groups = groups[:-1]
					continue
				else: 
					self.logger.warning("----------------------------------------------------")
					self.logger.warning(" METHODS ERROR in __confreader__)")
					self.logger.warning(" Found line with no message separator {} at line {} of file".format(self.mseparator, nl+1, confile.name))
					self.logger.warning("----------------------------------------------------")
					if self.pure : raise ValueError(" Found line with no message separator {} at line {} of file".format(self.mseparator, nl+1, confile.name))
					
			elif  self.__kcehc_pattern__(vm[0].strip(" \n\t\r"),self.mlvsymbol):
				command += vm[0].strip(" \n\t\r")[:-len(self.mlvsymbol)]
				message += vm[1].replace("\n"," ")
				if not continue_on: continue_on = nl+1
				continue
			else:
				command += vm[0]
				message += vm[1]				
				if self.__kcehc_pattern__(command.strip(" \n\t\r"),self.groupopen) or self.__kcehc_pattern__(command.strip(" \n\t\r"),self.groupend):
					self.logger.warning("----------------------------------------------------")
					self.logger.warning(" METHODS ERROR in __confreader__)")
					self.logger.warning(" Found group beginning or end before a message at line {} of {}".format(nl+1, confile.name))
					self.logger.warning(" NOTE: it you are using dictionaries please convert them into a subtree(s) to avoid this message again.")
					self.logger.warning("----------------------------------------------------")
				parts = resolve_expression( resolve_iterators(command) )+[message]
				parts = "/".join([""]+groups+[""])+parts[0].strip(" \n\t\r/"), parts[1].strip(" \n\t\r"),parts[2].strip(" \n\t\r")
				self.methods_txt[parts[0]]=parts[1],parts[2]
				command, message, continue_on = "", "", False
		confile.close()

	def __read_conf__(self,conf,parameters=None):
		try:
			xconf = methods(conf,"xconf",locals())
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in __read_conf__)")
			self.logger.error(" Cannot read configuration file {}: {}".format(conf,e))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise ValueError("Cannot read configuration file {}: {}".format(conf,e))
			return True
		if xconf.__resolve_iterators__():
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in __read_conf__)")
			self.logger.error(" Cannot resolve iterators in configuration file {}: {}".format(conf,e))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise ValueError("Cannot resolve iterators in configuration file {}: {}".format(conf,e))
			return True
		if parameters is None:
			for n in xconf.methods_txt:
				self.methods_txt[n] = xconf.methods_txt[n]
			return False
		else:
			if type(parameters) is str:
				parameters = [ parameters ]
			elif type(parameters) is list or type(parameters) is tuple:
				if reduce(lambda x,y: x and type(y) is str, parameters, True):	pass
				else:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in __read_conf__)")
					self.logger.error(" Not all parameters {} are strings".format(parameters))
					self.logger.error("----------------------------------------------------")		
					if self.pure : raise TypeError("Not all parameters {} are strings".format(parameters))
					return True
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __read_conf__)")
				self.logger.error(" Incorrect type of imported parameters. It should be string or list/tuple of string. {} is given".format(type(parameters)))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise TypeError("Incorrect type of exported parameters. It should be string or list/tuple of string. {} is given".format(type(parameters)))
				return True
			for param in parameters:
				if not param in xconf.methods_txt:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in __read_conf__)")
					self.logger.error(" Cannot import parameter {} from the configuration file {}: there is no such parameter".format(param,conf) )
					self.logger.error("----------------------------------------------------")		
					if self.pure : raise TypeError("Cannot import parameter {} from the configuration file {}: there is no such parameter".format(param,conf) )
					#return True # << Not sure what is the best option here
					continue
				self.methods_txt[param] = xconf.methods_txt[param]
			return False
					
				
	def __read_db__(self,dburl,record,parameters=None):
		try:
			d = db(dburl,mode="ro")
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in __read_db__)")
			self.logger.error(" Cannot open data base {}: {}".format(dburl,e))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise ValueError("Cannot open data base {}: {}".format(dburl,e))
			return True
		if parameters is None:
			try:
				xtree = d[record]
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __read_db__)")
				self.logger.error(" Cannot pool record {} from  data base {}: {}".format(record, dburl,e))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise ValueError("Cannot pool record {} from  data base {}: {}".format(record, dburl,e))
				return True
			for n in xtree:
				if type(xtree[n]) is str or type(xtree[n]) is  unicode:
					self.methods_txt[n] = xtree[n]
				else:
					self.methods[n] = xtree[n]
			return False
		else:
			if type(parameters) is str:
				parameters = [ parameters ]
			elif type(parameters) is list or type(parameters) is tuple:
				if reduce(lambda x,y: x and type(y) is str, parameters, True):	pass
				else:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in __read_db__)")
					self.logger.error(" Not all parameters {} are strings".format(parameters))
					self.logger.error("----------------------------------------------------")		
					if self.pure : raise TypeError("Not all parameters {} are strings".format(parameters))
					return True
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __read_db__)")
				self.logger.error(" Incorrect type of imported parameters. It should be string or list/tuple of string. {} is given".format(type(parameters)))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise TypeError("Incorrect type of exported parameters. It should be string or list/tuple of string. {} is given".format(type(parameters)))
				return True
			for param in parameters:
				try:
					xtree = d[record,param]
				except BaseException as e:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in __read_db__)")
					self.logger.error(" Cannot pool param {} from record {} in  data base {}: {}".format(param, record, dburl,e))
					self.logger.error("----------------------------------------------------")		
					if self.pure : raise ValueError("Cannot pool param {} from record {} in  data base {}: {}".format(param, record, dburl,e))
					return True
				for n in xtree:
					if type(xtree[n]) is str or type(xtree[n]) is  unicode:
						self.methods_txt[n] = xtree[n]
					else:
						self.methods[n] = xtree[n]
			return False
	
	def gethash(self, name=''):
		if not name in self.methods: return None
		if isinstance(self.methods[name], tree):
			achash=""
			for n in self.methods[name].dict():
				achash += ":"+self.gethash(name+self.hsymbol+n)
			return achash[1:]
		elif name in self.hashspace:
			return self.hashspace[name]
		elif self.is_lambda(self.methods[name]):
			if name in self.methods_txt:
				self.hashspace[name] = hashlib.sha1(self.methods_txt[name][0]).hexdigest()
				return self.hashspace[name]
			else:
				self.logger.warning(" > LAMBDA function not in namespace")
				self.hashspace[name] = hashlib.sha1(str(self.methods[name])).hexdigest()
				return self.hashspace[name]
		else:
			self.hashspace[name] = hashlib.sha1(str(self.methods[name])).hexdigest()
			return self.hashspace[name]
		return None

	# Functions for tree like behavior. 
	# They mostly redirect calls to self.methods tree
	def __setitem__(self, key, value): self.methods.__setitem__(key, value)
	def __getitem__(self, key)       : 
		if key[0] == "#":
			return self.gethash(key[1:])
		if not self.methods.__contains__(key) and self.methods_txt.__contains__(key):
			if self.generate(var=key):
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __getitem__)")
				self.logger.error(" Cannot resolve generate key {}".format(key))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise KeyError("Cannot resolve generate key {}".format(key))
				return None
		return self.methods.__getitem__(key)
	def __contains__(self,key)       : 
		return self.methods.__contains__(key) or self.methods_txt.__contains__(key)
	def __delitem__(self, key)       : self.methods.__delitem__(key)
	def __iter__(self):
		for name in self.methods.__iter__(): yield name
	def check(self, key)	         : return self.methods.check(key)
	def dict(self):
		for name in self.methods.dict():yield name
	def __namefilter__(self,name,flt):
		return reduce(lambda x,y: x or name.startswith(y), flt, False)

	# Functions which parse and convert text values to objects
	def __resolve_iterators__(self):
		"""
		resolves all iterators
		"""
		for itr in self.iterate:
			value = self.resolve_name(self.localcontext[self.target].methods, self.target, self.iterate[itr], prohibit=[])
			if value is None:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __resolve_iterators__)")
				self.logger.error(" Cannot resolve iterator {}".format(itr))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise ValueError("Cannot resolve iterator {}".format(itr))
				return True
			try:
				exec "{}.itrvalue={}".format(self.target,value) in self.localcontext
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in __resolve_iterators__)")
				self.logger.error(" Cannot execute operation {}.itrvalue={}: {}".format(self.target,value,e))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise ValueError("Cannot execute operation {}.itrvalue={}: {}".format(self.target,value,e))
				return True
			for name in self.methods_txt:
				if itr in name or itr in self.methods_txt[name][0]:
					for iv in self.itrvalue:
						self.methods_txt[name.replace(itr,"{}".format(iv))] = \
							self.methods_txt[name][0].replace(itr,"{}".format(iv)), self.methods_txt[name][1].replace(itr,"{}".format(iv))
			del self.itrvalue
		#cleanup text tree
		for name in self.methods_txt:
			if self.isymbol in name or self.isymbol in self.methods_txt[name]:
				del self.methods_txt[name]
		return False
	
	def builder(self, tree, target, item, delimiter, prohibit):
		"""
		Finds and resolves build expressions for links and strings
		"""
		if delimiter not in item: return item
		result = ''
		copir = item.split(delimiter)
		#BeetDemGuise sugessted to use zip instead map 
		# (see http://codereview.stackexchange.com/questions/52729/configuration-file-with-python-functionality).
		# But zip function returns shortest argument 
		# sequence. So, the 'tail' of item after last
		# delimiter just disappiers in the result and raise
		# an error. Any ideas?
		for prefix,var in map(None,copir[::2],copir[1::2]):
			if prefix is not None: result += prefix
			if var is None: continue
			if not var in tree and var in self.methods_txt:
				if self.__namefilter__(var, prohibit):
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in generate)")
					self.logger.error(" Parameter \'{}\' wasn't resolved and it is prohibited.".format(var))
					self.logger.error("----------------------------------------------------")		
					if self.pure: raise TypeError("Parameter \'{}\' wasn't resolved and it is prohibited.".format(var))
					return None
				self.stackcnt += 1
				if self.stackcnt > 100:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in generate)")
					self.logger.error(" Exceed number of stack operation (101).")
					self.logger.error("----------------------------------------------------")		
					if self.pure: raise ValueError("Exceed number of stack operation (101).")
					return None
				self.generate(var,target=self.target,localcontext=self.localcontext, prohibit=prohibit) 
			lmdcheck = self.is_lambda(tree[var])
			if lmdcheck or delimiter is self.refsymbol:
				result += target+".methods[\""+var+"\"]"
			elif delimiter is self.strefsymbol:
				if var in self.methods_txt:
					result +=  self.methods_txt[var][0]
				else:
					result += str(tree[var])
			elif delimiter is self.refhash:
				result += "\'\\\'{}\\\'\'".format(self.gethash(var))
			else:
				return None
			self.dependences.append(var)
		return result

	def resolve_name(self, tree, target, item, prohibit):
		"""
		Resolves links, string and hashes in RHS of parameters
		"""
		# Resolve links First
		# then Resolve strings
		# and then Resolve hashs
		result = item
		for delimiter in self.refsymbol, self.strefsymbol, self.refhash:
			bld = self.builder(tree, target, result, delimiter , prohibit=prohibit)
			if bld is None:	return None
			result = bld
		return unicode(result)

	def generate(self, var=None, target=None, localcontext = None, prohibit=None, text=False):
		"""
		generates python objects in self.methods tree based on records in self.methods_txt tree
		@opt   var         - variable or list of variables which should be generated
		@opt   target      - string of variable name which will be generated (need for lambda(s)), 
		@opt   localcontext- local name space, usually = globals() or locals()
		@opt   prohibit    - list of name which shouldn't be generated
		@opt   text        - bool If true populate tree by strings not actual values
		"""
		self.target = self.dtarget if target is None else target

		if   not localcontext is None:   self.localcontext = localcontext
		elif not self.dlocalctx is None: self.localcontext = self.dlocalctx
		else:                            self.localcontext = globals()
		
			
		if not self.target in self.localcontext:			
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in generate)")
			self.logger.error(" Target object \'{}\' is not found in context".format(self.target))
			self.logger.error("----------------------------------------------------")		
			if self.pure: raise ValueError("Target object \'{}\' is not found in context".format(self.target))
			return True


		if not prohibit is None:
			if   type(prohibit) is str:                             prohibit = [ prohibit ]
			elif type(prohibit) is list or type(prohibit) is tuple: 
				if reduce(lambda x,y: x and type(y) is str, prohibit, True) : pass  
				else:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" METHODS ERROR in generate)")
					self.logger.error(" One of entrances of the Prohibit option has wrong type.")
					self.logger.error("----------------------------------------------------")		
					if self.pure: raise TypeError("One of entrances of the Prohibit option has wrong type.")
					return True

			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in generate)")
				self.logger.error(" Prohibit option has wrong type \'{}\'.".format(type(prohibit)))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Prohibit option has wrong type \'{}\'.".format(type(prohibit)))
				return True
		else : prohibit = []
				
		
		if   var is None:                             var = self.methods_txt
		elif type(var) is list or type(var) is tuple: pass
		elif type(var) is str                       : var = [ var ]
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" METHODS ERROR in generate)")
			self.logger.error(" Variable option has wrong type \'{}\'; it should be a string or a list of strings or None.".format(type(var)))
			self.logger.error("----------------------------------------------------")		
			if self.pure                 : raise TypeError("Variable option has incorrect type \'{}\'.".format(type(prohibit)))
			return True

		if not self.iterate_res	:
			self.iterate_res = True
			if self.__resolve_iterators__():
				self.iterate_res = False
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in generate)")
				self.logger.error(" Cannot resolve iterators")
				self.logger.error("----------------------------------------------------")		
				if self.pure              : raise ValueError("Cannot resolve iterators")
				return True
			
			
		for name in var:
			if not type(name) is str:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in generate)")
				self.logger.error(" Variable name is not string: type \'{}\' is given.".format(type(name)))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Variable name is not string: type \'{}\' is given.".format(type(name)))
				return True
			if self.__namefilter__(name, prohibit):
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in generate)")
				self.logger.error(" Resolving a name \'{}\' is prohibited.".format(name))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Resolving a name \'{}\' is prohibited.".format(name))
				return True
			if not name in self.methods_txt:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" METHODS ERROR in generate)")
				self.logger.error(" Cannot find parameter name \'{}\' is the text tree.".format(name))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Cannot find parameter name \'{}\' is the text tree.".format(name))
				return True
				
					
			value = self.methods_txt[name]
			if isinstance(value, tree):
				for n in value:
					self.generate(name+n,target=self.target, localcontext = self.localcontext, prohibit=prohibit, text=text)
			else:
				value = value[0]
				self.dependences = []
				if text:				
					if not self.hashspace.check(name):
						self.hashspace[name] = hashlib.sha1(value).hexdigest()
						pass
					try:
						exec "{}.methods[\'{}\']=\"{}\"".format(self.target,name,re.sub(r"\\", "\\\\", re.sub(r"\"","\\\"", re.sub("\'","\\\'", value) ) )) in self.localcontext
					except BaseException as e:
						self.logger.error("----------------------------------------------------")
						self.logger.error(" METHODS ERROR in generate)")
						self.logger.error(" Cannot execute operation {}[\'{}\']=\"{}\": {}".format(
							target, name,
							re.sub(r"\\", "\\\\", re.sub(r"\"","\\\"", re.sub("\'","\\\'", value))),e))
						self.logger.error("----------------------------------------------------")		
						if self.pure             : raise ValueError("Cannot execute operation {}[\'{}\']=\"{}\": {}".format(target,name,re.sub(r"\\", "\\\\", re.sub(r"\"","\\\"", re.sub("\'","\\\'", value))),e))
						return True
				else:
					value = self.resolve_name(self.localcontext[self.target].methods, self.target,value,prohibit=prohibit)
					try:
						exec "{}.methods[\'{}\']={}".format(self.target,name,value) in self.localcontext
					except BaseException as e:
						self.logger.error("----------------------------------------------------")
						self.logger.error(" METHODS ERROR in generate)")
						self.logger.error(" Cannot execute operation {}[\'{}\']={}: {}".format(self.target,name,value,e))
						self.logger.error("----------------------------------------------------")		
						if self.pure             : raise ValueError("Cannot execute operation {}[\'{}\']={}: {}".format(self.target,name,value,e))
						return True
					#if not self.hashspace.check(name):
						#self.hashspace[name] = self.gethash(name)
					#self.hashspace[name] = self.gethash(name)
					## Update hash everytime then parameter change 
					## NOTE: it doesn't help with data set up outside methods
					self.hashspace[name] = self.gethash(name)#hashlib.sha1(str(self.methods[name])).hexdigest()
					for dep in self.dependences:
						self.hashspace[name] += ":"+self.gethash(dep)
				self.dependences = []
				self.logger.debug( " > % 76s : OK"%(name))
		return False

	



if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "USEAGE: python -m ../simtoolkit/methods model-fileformats/general-syntax.stkconf"
		exit(1)
	#CHECK LOGGER
	logging.basicConfig(format='%(asctime)s: %(levelname)-8s:%(message)s', level=logging.INFO)
	m = methods(sys.argv[1:],"m",globals())
	print
	print m.mainmessage.replace("\n", "\nMESSAGE:   ")
	print
	print "THE TEXT TREE (BEFORE FIRST RESOLVING)"
	for p,k in m.methods_txt.printnames():
		if k is None:
			print p
		else:
			print p,m.methods_txt[k]

	print
	print "THE ITEERATIORS"
	for p in m.iterate:
		print p,m.iterate[p]
	
	#help(m)

	print "#====================================#"
	print "#            ERROR CHECKS            #"
	m.generate(target="x",localcontext=globals())
	m.generate(prohibit=10)
	m.generate(prohibit=['/a','/b',30])
	m.generate(10)
	m.generate([12,'/parameter1','/goup2/parameter2'])
	m.generate('/a',prohibit=['/a','/b'])
	m.generate('/a/b/c',prohibit=['/a','/b'])
	m.generate('/ppy')
	print "#====================================#"
	print
	print "THE TEXT TREE (AFTER SOME RESOLVING)"
	for p,k in m.methods_txt.printnames():
		if k is None:
			print p
		else:
			print p,m.methods_txt[k]

	print
	print "#====================================#"
	print "#       RESOLVING PARAMETERS         #"
	m.generate("/parameter1")
	m.generate(["/group","/group2","/group3"])
	m.generate("/calc2")
	m.generate("/types")
	m.generate("/MyGroup")
	m.generate("/MultiIter")
	m.generate(["/L","/P"])
	m.generate(["/2by2is",'/calc',"/Multiline/Item"])
	m.generate("/str")
	m.generate("/hash")
	print 
	print "#====================================#"
	print
	print "THE VALUE TREE"
	for p,k in m.methods.printnames():
		if k is None:
			print p
		else:
			print p,m.methods[k]
	print "#====================================#"
	print "#         RESOLVING AS TEXT          #"
	m.generate(text=True)
	print 
	print "#====================================#"
	print
	print "THE TEXT VALUE TREE"
	for p,k in m.methods.printnames():
		if k is None:
			print p
		else:
			print p,m.methods[k],type(m.methods[k])
	
	
	
	##>> Remove a /STIMULI to show resolving in line
	del m["/STIMULI"]
	print "================================================================================="
	print "#                                       RESOLVED INLINE                         #"
	print "#                                      THE STIMULI TREE :                       #"	
	for p in m["/STIMULI"]:
		print "% 55s :"%("/STIMULI"+p),m["/STIMULI"+p]
	print "================================================================================="
