import os, sys, types, logging, re, hashlib
from collections import OrderedDict
from simtoolkit.tree import tree
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
	"""
	is_lambda = lambda self, value    : isinstance(value, types.LambdaType) and value.__name__ == '<lambda>'
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
		                   - default parameter set (may a list of names or open files).
		@param target      - name of methods object which will be created by this constructor
		@param localcontext- local namespace where all exec(s) should be run.
		                   - can be alter by localcontext parameter in generate function
		                   - if None, methods uses current context from globals()
		---
		@opt   argvs       - command line arguments, which will be processed after defaults .
		@opt   dbsymbol    - (a set of) symbol(s) to indicate that some parameters should be read
						   - from data base 
		@opt   dbsepcartor - a separator to segregate database name/url, record hash/time-stamp and
		                   - parameter name(s) 
		                   - EXAMPLES:
						       url access, extracts record using time-stamp:
		                          db=url:myneuron.org:(2016,03,02):/Populations/E:/Population/P:/Connections
		                       local STKDB file access, extracts record using hash:
		                          db=simulations.stkdb:2a077d01a8a1018c6902b20b8bcfe1e90082b952:/Populations/AN:/Populations/SBC:/Populations/GBC
		@opt   cfsymbol    - (a set of) symbol(s) to indicate that some parameters should be read
						   - from additional configuration file 
		@opt   cfseparator - a separator to segregate configuration filename  and
		                   - parameter name(s) 
		                   - EXAMPLE:
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
			self.logger.error("SimToolKit: METHODS ERROR(methods.__init__)")
			self.logger.error("          : Confile should be a string or a file object or list/tuple of strings or file objects. {} is given".format(type(confile)))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise TypeError("Confile should be a string or a file object or list/tuple of strings or file objects. {} is given".format(type(confile)))
		for cf in confile:
			self.__confreader__(cf)
		
		
	def __confreader__(self, confile):
		"""
		reads configuration file(s) and creates text tree.
		it resolves groups and prepare iterators for farther use
		"""
		def check(key,pat):
			return key[ :len(pat)] == pat
		def kcehc(key,pat):
			return key[-len(pat):] == pat
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


		if not ( type(confile) is str or type(confile) is file ) :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: METHODS ERROR(methods.__confreader__)")
			self.logger.error("		     : Confile should be a string or a file object. {} is given".format(type(confile)))
			self.logger.error("----------------------------------------------------")		
			if self.pure : raise ValueError("Confile should be a string or a file object. {} is given".format(type(confile)))
		if type(confile) is str:
			confile = open(confile)

		
		message_on = False
		groups = []
		command, message, continue_on = "", "", False
		#Step one read parameters tree from the file
		for nl,line in enumerate(confile.readlines()):
			l = line.strip("\n\r")
			if len(l)             == 0               : continue
			if l.lstrip(" \t")[0] == self.mseparator : continue
			#Extracting the main message 
			if check(l,self.mmopen):
				message_on        = True
				self.mainmessage += l[len(self.mmopen):]+"\n"
				continue
			elif message_on and kcehc(l,self.mmend):
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
				if kcehc(vm,self.groupopen):
					#resolve ierators in a group
					gname = resolve_iterators(vm[:-len(self.groupopen)].strip(" \n\t\r"+self.hsymbol)) 
					groups.append(gname)
					continue
				elif kcehc(vm,self.groupend):
					groups = groups[:-1]
					continue
				else: 
					self.logger.error("----------------------------------------------------")
					self.logger.error("SimToolKit: METHODS ERROR(methods.__confreader__)")
					self.logger.error("          : Found line with no message separator {} at line {} of file".format(self.mseparator, nl+1, confile.name))
					if self.pure : raise ValueError(" Found line with no message separator {} at line {} of file".format(self.mseparator, nl+1, confile.name))
					self.logger.warning("----------------------------------------------------")
			elif  kcehc(vm[0].strip(" \n\t\r"),self.mlvsymbol):
				command += vm[0].strip(" \n\t\r")[:-len(self.mlvsymbol)]
				message += vm[1].replace("\n"," ")
				if not continue_on: continue_on = nl+1
				continue
			else:
				command += vm[0]
				message += vm[1]				
				if kcehc(command.strip(" \n\t\r"),self.groupopen) or kcehc(command.strip(" \n\t\r"),self.groupend):
					self.logger.warning("----------------------------------------------------")
					self.logger.warning("SimToolKit: METHODS ERROR(methods.__confreader__)")
					self.logger.warning("          : Found group beginning or end before a message at line {} of {}".format(nl+1, confile.name))
					self.logger.warning("          : NOTE: it you are using dictionaries please convert them into a subtree(s) to avoid this message again.")
					self.logger.warning("----------------------------------------------------")
				parts = resolve_expression( resolve_iterators(command) )+[message]
				parts = "/".join([""]+groups+[""])+parts[0].strip(" \n\t\r/"), parts[1].strip(" \n\t\r"),parts[2].strip(" \n\t\r")
				self.methods_txt[parts[0]]=parts[1],parts[2]
				command, message, continue_on = "", "", False
		confile.close()

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

#### FROM SIM TOOLS METHODS >>>>>
	def __setitem__(self, key, value): self.methods.__setitem__(key, value)
	def __getitem__(self, key)       : 
		if key[0] == "#":
			return self.gethash(key[1:])
		return self.methods.__getitem__(key)
	def __contains__(self,key)       : return self.methods.__contains__(key)
	def __delitem__(self, key)       : self.methods.__delitem__(key)
	def __iter__(self):
		for name in self.methods.__iter__(): yield name
	def check(self, key)	         : return self.methods.check(key)
	def dict(self):
		for name in self.methods.dict():yield name
#### <<<<<<<<<<<<<<<<<<<<<<<<<<<<		
	def __namefilter__(self,name,flt):
		return reduce(lambda x,y: x or name.startswith(y), flt, False)

	def __resolve_iterators__(self):
		"""
		resolves all iterators
		"""
		for itr in self.iterate:
			value = self.resolve_name(self.localcontext[self.target].methods, self.target, self.iterate[itr], prohibit=[])
			if value is None:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.__resolve_iterators__)")
				self.logger.error("          : Cannot resolve iterator {}".format(itr))
				self.logger.error("----------------------------------------------------")		
				if self.pure : raise ValueError("Cannot resolve iterator {}".format(itr))
				return True
			try:
				exec "{}.itrvalue={}".format(self.target,value) in self.localcontext
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.__resolve_iterators__)")
				self.logger.error("		     : Cannot execute operation {}.itrvalue={}: {}".format(self.target,value,e))
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
					self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
					self.logger.error("		     : Parameter \'{}\' wasn't resolved and it is prohibited.".format(var))
					self.logger.error("----------------------------------------------------")		
					if self.pure                 : raise TypeError("Parameter \'{}\' wasn't resolved and it is prohibited.".format(var))
					return None
				self.stackcnt += 1
				if self.stackcnt > 100:
					self.logger.error("----------------------------------------------------")
					self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
					self.logger.error("		     : Exceed number of stack operation (101).")
					self.logger.error("----------------------------------------------------")		
					if self.pure                 : raise ValueError("Exceed number of stack operation (101).")
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
		generates records in the methods tree
		@opt   var         - variable or list of variables which should be generated
		@opt   target      - string of variable name which will be generate (need for lambda(s)), 
		@opt   localcontext- local name space, usually = globals() or locals()
		@opt   prohibit    - list of name which should be generated
		@opt   text        - bool If true populate tree by strings not actual values
		"""
		self.target = self.dtarget if target is None else target

		if   not localcontext is None:   self.localcontext = localcontext
		elif not self.dlocalctx is None: self.localcontext = self.dlocalctx
		else:                            self.localcontext = globals()
		
			
		if not self.target in self.localcontext:			
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
			self.logger.error("		     : Target object \'{}\' is not found in context".format(self.target))
			self.logger.error("----------------------------------------------------")		
			if self.pure                 : raise ValueError("Target object \'{}\' is not found in context".format(self.target))
			return True


		if not prohibit is None:
			if   type(prohibit) is str:                             prohibit = [ prohibit ]
			elif type(prohibit) is list or type(prohibit) is tuple: 
				if reduce(lambda x,y: x and type(y) is str, prohibit, True) : pass  
				else:
					self.logger.error("----------------------------------------------------")
					self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
					self.logger.error("		     : One of entrances of the Prohibit option has wrong type.")
					self.logger.error("----------------------------------------------------")		
					if self.pure                 : raise TypeError("One of entrances of the Prohibit option has wrong type.")
					return True

			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
				self.logger.error("		     : Prohibit option has wrong type \'{}\'.".format(type(prohibit)))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Prohibit option has wrong type \'{}\'.".format(type(prohibit)))
				return True
		else :                                                      prohibit = []
				
		
		if   var is None:                             var = self.methods_txt
		elif type(var) is list or type(var) is tuple: pass
		elif type(var) is str                       : var = [ var ]
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
			self.logger.error("		     : Variable option has wrong type \'{}\'; it should be a string or a list of strings or None.".format(type(var)))
			self.logger.error("----------------------------------------------------")		
			if self.pure                 : raise TypeError("Variable option has incorrect type \'{}\'.".format(type(prohibit)))
			return True
		
		if not self.iterate_res	:
			self.iterate_res = True
			if self.__resolve_iterators__():
				self.iterate_res = False
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
				self.logger.error("		     : Cannot resolve iterators")
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise ValueError("Cannot resolve iterators")
				return True
			
			
		for name in var:
			if not type(name) is str:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
				self.logger.error("		     : Variable name is not string: type \'{}\' is given.".format(type(name)))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Variable name is not string: type \'{}\' is given.".format(type(name)))
				return True
			if self.__namefilter__(name, prohibit):
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
				self.logger.error("		     : Resolving a name \'{}\' is prohibited.".format(name))
				self.logger.error("----------------------------------------------------")		
				if self.pure                 : raise TypeError("Resolving a name \'{}\' is prohibited.".format(name))
				return True
			if not name in self.methods_txt:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
				self.logger.error("		     : Cannot find parameter name \'{}\' is the text tree.".format(name))
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
						self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
						self.logger.error("		     : Cannot execute operation {}[\'{}\']=\"{}\": {}".format(
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
						self.logger.error("SimToolKit: METHODS ERROR(methods.generate)")
						self.logger.error("		     : Cannot execute operation {}[\'{}\']={}: {}".format(self.target,name,value,e))
						self.logger.error("----------------------------------------------------")		
						if self.pure             : raise ValueError("Cannot execute operation {}[\'{}\']={}: {}".format(self.target,name,value,e))
						return True
					#if not self.hashspace.check(name):
						#self.hashspace[name] = self.gethash(name)
					#self.hashspace[name] = self.gethash(name)
					## Update hash everytime then parameter change 
					## NOTE: it doesn't help with data set up outside methods
					#>> get it back!!!!!
					self.hashspace[name] = self.gethash(name)#hashlib.sha1(str(self.methods[name])).hexdigest()
					for dep in self.dependences:
						self.hashspace[name] += ":"+self.gethash(dep)
					#<<
				self.dependences = []
				self.logger.debug( " > % 76s : OK"%(name))
		return False




if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "USEAGE: python -m ../simtoolkit/methods model-fileformats/general-syntax.stkconf"
		exit(1)
	#CHECK LOGGER
	logging.basicConfig(format='%(asctime)s: %(levelname)-8s:%(message)s', level=logging.DEBUG)
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
	
	print "#====================================#"
	print "#         RESOLVING STIMULI          #"
	m.generate("/STIMULI")
	print "====================================== THE STIMULI TREE : ====================="
	#for p,k in m.methods.printnames():
		#if k is None:
			#print p
		#else:
			#print p,m.methods[k],type(m.methods[k])
	for p in m["/STIMULI"]:
		print "% 55s :"%("/STIMULI"+p),m["/STIMULI"+p]
