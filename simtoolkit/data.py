import os, sys, types, logging, hashlib, zlib, threading, io, time, struct, psutil, platform
from multiprocessing import Process, Queue 
import mmap
import numpy as np
from urlparse import urlparse

from simtoolkit.tree import tree

logging.DEEPDEBUG = 5
logging.addLevelName(logging.DEEPDEBUG, "DEEPDEBUG")
logging.Logger.deepdebug = lambda inst, msg, *args, **kwargs: inst.log(logging.DEEPDEBUG, msg, *args, **kwargs)
logging.deepdebug = logging.Logger.deepdebug

class data:
	def __init__(self, durl, mode="r+", username="", password="", architecture=platform.machine(), **kwargs):
		"""
		data is an switcher, which allows to use the same interface for many different possible data storage mechanisms
		   and formats.
		"""
		self.logger = logging.getLogger("simtoolkit.data.data")
		self.durl = durl
		self.mode  = "w+"
		up = urlparse(durl)
		self.dtype = "file" if up.scheme == "" else up.scheme
		if up.query != "":
			upq = dist( urlparse.parse_qsl(up.query) )
			if "mode" in upq: self.mode = upq['mode']
		self.path     = dburl if up.path == "" else up.path
		self.username = up.username
		self.password = up.password
		
		if type(mode) is str:
			if mode     != "": self.mode     = mode.lower()
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in __init__")
			self.logger.error(" Incorrect type of mode argument. It should be a str. {} is given".format(type(mode)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect type of mode argument. It should be a str. {} is given".format(type(mode)))
		if type(username) is str:
			if username != "": self.username = username
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in __init__")
			self.logger.error(" Incorrect type of username argument. It should be a str. {} is given".format(type(username)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect type of username argument. It should be a str. {} is given".format(type(username)))
		if type(password) is str:
			if password != "": self.password = password
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in __init__")
			self.logger.error(" Incorrect type of password argument. It should be a str. {} is given".format(type(password)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect type of password argument. It should be a str. {} is given".format(type(password)))
		
		#Default values
		if self.dtype == "" : self.dtype = "file"

		if self.dtype == "file":
			if os.path.isdir(self.path):
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in __init__")
				self.logger.error(" The {} is a directory".format(self.path))
				self.logger.error("----------------------------------------------------")		
				raise ValueError("The {} is a directory".format(self.path))
			cmd = {}
			# Coppy and use only relevant to data_file key parameters
			for i in 'compress','npcompress','parallel','maxbuffersize','autocorrection','autodefragmentation':
				if i in kwargs:
					cmd[i]=kwargs[i]
			if   self.mode == "r+" or self.mode == "a" or self.mode == "wr" or self.mode == "rw":
				if os.path.exists(self.path) and not os.access(self.path, os.W_OK):
					self.logger.warning("----------------------------------------------------")
					self.logger.warning(" DATABASE ERROR in __init__")
					self.logger.warning(" File {} is read-only - open in ro mode".format(self.path))
					self.logger.warning("----------------------------------------------------")		
					self.data = data_file(self.path, mode="ro", **cmd)
				else:
					self.data = data_file(self.path, mode="r+",**cmd)
			elif self.mode == "w":
				if os.path.exists(self.path):
					if not os.access(self.path, os.W_OK):
						self.logger.error("----------------------------------------------------")
						self.logger.error(" DATA ERROR in __init__")
						self.logger.error(" The file {} is read-only. Cannot open it in 'w' mode".format(self.path))
						self.logger.error("----------------------------------------------------")		
						raise ValueError("The file {} is read-only. Cannot open it in 'w' mode".format(self.path))
				self.data = data_file(self.path, mode="w",  **cmd)
			elif self.mode == "ro":
				self.data = data_file(self.path, mode="ro", **cmd)
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in __init__")
				self.logger.error(" Unknown mode {}".format(self.mode))
				self.logger.error(" mode should be 'r+', 'w', or 'ro'")
				self.logger.error("----------------------------------------------------")		
				raise ValueError("Unknown mode {}".format(self.mode))
		#elif self.dbtype == "hdf5"
		#elif self.dbtype == "data-server"
		#elif self.dbtype == "something-else-to-think-about"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATAE ERROR in __init__")
			self.logger.error(" Data base connector for {} isn't implemented yet".format(self.dbtype))
			self.logger.error("----------------------------------------------------")		
			raise ValueError("Data base connector for {} isn't implemented yet".format(self.dbtype))
		#Redirection to the data class
		self.__enter__       = self.data.__enter__
		self.__exit__        = self.data.__exit__
		self.sync            = self.data.sync
		self.__len__         = self.data.__len__
		self.__add__         = self.data.__add__
		self.__iadd__        = self.data.__iadd__
		self.__setitem__     = self.data.__setitem__
		self.__getitem__     = self.data.__getitem__
		self.__delitem__     = self.data.__delitem__
		self.__call__        = self.data.__call__
		self.__contains__    = self.data.__contains__
		self.__iter__        = self.data.__iter__
		self.aggregate       = self.data.aggregate
		self.dict            = self.data.dict
		self.defragmentation = self.data.defragmentation
		#self.set             = self.data.set

		#---   Information   ---
		#self.info         = self.db.info


class data_file:
	def __init__(self, filename, mode="r+", compress = 5, npcompress=False, parallel=False, maxbuffersize=0, autocorrection=False, autodefragmentation=False):
		self.logger = logging.getLogger("simtoolkit.data.data_file")
		self.logger.deepdebug(" > Open simdata: file={}, modw={}, compress={}, parallel={}, npcompress={}, maxbuffersize={}".format(filename, mode, compress, parallel, npcompress,maxbuffersize))
		self.filename = filename
		self.autocorrection = autocorrection
		self.autodefragmentation = autodefragmentation
		self.mode = mode
		if mode != "w":
			self.readfooter()				
		else:
			self.initfooter()

		self.bufdata = []
		self.compres = compress
		self.parallel = parallel
		self.npcompress = npcompress
		self.maxbufsize = maxbuffersize
		if self.maxbufsize < 1:
			#self.maxbufsize = psutil.virtual_memory().total/4
			#>>
			self.maxbufsize = psutil.virtual_memory().available/4
			#<<
			#self.logger.deepdebug(" < Receiver: maxbuffersize={}".format(self.maxbufsize))
			self.logger.deepdebug(" - Both    : maxbuffersize={}".format(self.maxbufsize))
		if self.parallel:
			#self.queue   = Queue(maxsize = self.maxbufsize/10)
			#self.queue   = Queue(self.maxbufsize/10)
			self.queue   = Queue(21)
			self.dthread = Process(target=self.savebuffer) 
			self.dthread.start()
			self.process = psutil.Process()
			self.logger.deepdebug(" > Sender  : pid={}".format(self.process.pid ))
			
			self.__exit__     = self.p__exit__
			self.__setitem__  = self.pset
			self.sync         = self.psync
		else:
			self.__exit__     = self.s__exit__
			self.__setitem__  = self.sset
			self.sync         = self.ssync
		

	def initfooter(self):
		self.datamap = tree()
		self.mtime   = time.time()
		self.tail    = 0
		self.fsize   = 0
		if self.filename is None or self.mode == "ro": return
		try:
			with open(self.filename,"wb") as fd: pass
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in initfooter")
			self.logger.error(" File \'{}\' cannot be written: {}".format(self.filename,e))
			self.logger.error("----------------------------------------------------")		
			raise ValueError("File \'{}\' cannot be written: {}".format(self.filename,e))

	def readfooter(self):
		if self.filename is None:
			return self.initfooter()
		try:
			self.fsize = os.path.getsize(self.filename)
		except:
			self.initfooter()
		
		if self.fsize > 8:
			try:
				with open(self.filename,"rb") as fd:
					fd.seek(-8,2)                            #everything from the tail
					self.logger.deepdebug(" > rdft: shits to -8,2")
					idx =  struct.unpack(">Q",fd.read(8))[0] #Tree size
					self.logger.deepdebug(" > rdft: idx(treesize)={}".format(idx))
					fd.seek(-idx-8,2)
					self.logger.deepdebug(" > rdft: shits to -{}-8,2={}".format(idx,(-idx-8,2)))
					#importing back to the tree
					self.datamap = tree().imp( zlib.decompress(fd.read(idx)) ) 
					self.logger.deepdebug(" > rdft: the tree={}".format(self.datamap))
			except BaseException as e:
				self.logger.warning("----------------------------------------------------")
				self.logger.warning(" DATA ERROR in readfooter")
				self.logger.warning(" Cannot open file \'{}\': {}".format(self.filename,e))
				self.logger.warning("----------------------------------------------------")		
				if not self.autocorrection:
					raise RuntimeError("Cannot open file \'{}\': {}".format(self.filename,e))
				else:
					self.rescan_file()
				#self.initfooter()
		else:
			self.initfooter()
		self.mtime = os.stat(self.filename).st_mtime
		self.tail  = 0
		for n in self.datamap:
			for fl,st,sz,tp in self.datamap[n]:
				if not fl is None: continue
				if self.tail <= st+sz: self.tail = st+sz #+1
		self.logger.deepdebug(" > rdtf: self.tail={}".format(self.tail))

	def rescan_file(self):
		self.datamap = tree()
		self.tail    = 0
		with open(self.filename,"rw+b") as fd:
			mm = mmap.mmap(fd.fileno(), 0)
			start = mm.find('#STKDATA')
			while start >= 0:
				chheadersize, = struct.unpack(">H",mm[start+8:start+10])
				sz,ch,ty,name = eval(mm[start+10:start+10+chheadersize])
				st = start+10+chheadersize
				Xch = 0 if not name in self.datamap else len(self.datamap[name])
				if Xch != ch:
					self.logger.error("----------------------------------------------------")
					self.logger.error(" DATA ERROR in repare_file")
					self.logger.error(" Chunk number {} of variable {} is not correct - should be".format(ch,name,Xch))
					self.logger.error("----------------------------------------------------")
				#checking chunk size
				self.tail = start+10+chheadersize+sz #+1
				if mm[self.tail:self.tail+8] == '#STKDATA':
					start = self.tail
				else:
					start = mm.find('#STKDATA',start+1)
					#TODO: recalculate actual data
					if start > 0:
						self.logger.error("----------------------------------------------------")
						self.logger.error(" DATA ERROR in repare_file")
						self.logger.error(" Chunk {} of variable {} has a wrong size : {} - skip it".format(Xch,name,sz))
						self.logger.error("----------------------------------------------------")
						continue
					
				if name in self.datamap:
					self.datamap[name].append( [None,st,sz,ty] )
				else:
					self.datamap[name] = [ [None,st,sz,ty] ] 		
			#TODO: sort every name
			#TODO: 
			self.writefooter()

	def writefooter(self):
		if self.filename is None:
			self.logger.warning("----------------------------------------------------")
			self.logger.warning(" DATA ERROR in writefooter")
			self.logger.warning(" Cannot write footer into virtual file")
			self.logger.warning("----------------------------------------------------")
			return
		if self.mode == "ro":
			self.logger.warning("----------------------------------------------------")
			self.logger.warning(" DATA ERROR in writefooter")
			self.logger.warning(" Cannot write footer into read only 'ro' file")
			self.logger.warning("----------------------------------------------------")
			return
		with open(self.filename,"rb+") as fd:
			self.logger.deepdebug(" > wrft: self.tail={}".format(self.tail))
			fd.seek( self.tail )
			footer = zlib.compress(str(self.datamap.exp()),9)
			fd.write(footer)
			fd.write(struct.pack(">Q",len(footer)) )
			fd.truncate()
			fd.flush()
			
			
		self.mtime = os.stat(self.filename).st_mtime
	

	def zipper(self, name, data):
		if type(data) is str or type(data) is unicode:
			if self.compres:
				return name, zlib.compress(data,self.compres),"ZSTRING"
			else:
				return name, data,"STRING"	
		elif not isinstance(data,np.ndarray):
			if self.compres:
				return name, zlib.compress(str(data),self.compres),"ZPYTHON"
			else:
				return name, str(data),"PYTHON"
		else:
			with io.BytesIO() as fd:
				np.save(fd, data)
				if self.npcompress:
					return name, zlib.compress(fd.getvalue(),self.npcompress) ,"ZNUMPY"
				else:
					return name, fd.getvalue() ,"NUMPY"

	def __save_chunk__(self,name,data,datatype):
		if self.filename is None:
			self.logger.warning("----------------------------------------------------")
			self.logger.warning(" DATA ERROR in __save_chunk__")
			self.logger.warning(" Cannot save data into virtual file")
			self.logger.warning("----------------------------------------------------")
			return
		if self.mode == 'ro':
			self.logger.warning("----------------------------------------------------")
			self.logger.warning(" DATA ERROR in __save_chunk__")
			self.logger.warning(" Cannot save data into read only 'ro' file")
			self.logger.warning("----------------------------------------------------")
			return
		#if self.mtime != os.stat(self.filename).st_mtime: self.readfooter()
		with open(self.filename,"rb+") as fd:
			datasize = len(data)
			chn = len(self.datamap[name]) if name in self.datamap else 0
			fd.seek(self.tail)
			chheader = str([ datasize,chn,datatype,	name ])
			chheadersize = struct.pack(">H",len(chheader))
			fd.write("#STKDATA")
			fd.write(chheadersize)
			fd.write(chheader)
			fd.write(data)
			chrec = ( None, self.tail+10+len(chheader), datasize, datatype )
			if name in self.datamap:
				self.datamap[name].append(chrec)
			else:
				self.datamap[name] = [chrec]
			self.tail += 10 + len(chheader) + datasize #+1 
		return self
	def __enter__(self)                                : return self
	def __len__(self) :
		cnt=0
		for n in self: cnt += 1
		return cnt
	def __add__(self,xdata):
		"Creating a new object add aggregate all data there"
		newdata = data(None)
		newdata.aggregate(self, xdata)
		return newdata
	def __iadd__(self,xdata):
		"Aggregating all data in this object += operator"
		if type(xdata) is list or type(xdata) is tuple:
			self.aggregate(*xdata)
		else:
			self.aggregate(xdata)
		return self
			
	# serial (not parallel) functions
	def s__exit__(self, exc_type, exc_value, traceback): self.writefooter()
	def sset(self,name,data)                           : return self.__save_chunk__( *self.zipper( name, data ) )
	def ssync(self)                                    : self.writefooter()
	
	def __read_chunk__(self,fl,st,sz,tp):
		self.logger.deepdebug(" > reading chunk: file={}, start={}, size={},type={}".format(fl,st,sz,tp) )
		self.logger.deepdebug(" > reading chunk: open file={}".format(self.filename if fl is None else fl))
		with open(self.filename if fl is None else fl,"rb") as fd:
			fd.seek(st)
			if   tp == "ZNUMPY":
				return np.load(io.BytesIO(zlib.decompress(fd.read(sz)))) 
			elif tp == "NUMPY":
				return  np.load(io.BytesIO(fd.read(sz)))
			elif tp == "ZPYTHON":
				return  eval(zlib.decompress(fd.read(sz)))
			elif tp == "PYTHON":
				return  eval(fd.read(sz))
			elif tp == "ZSTRING":
				return  zlib.decompress(fd.read(sz))
			elif tp == "STRING":
				return  fd.read(sz)
			else: 
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in __read_chunk__")
				self.logger.error(" Unsupported data format for name {}: {}".format(name, tp))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Unsupported data format for name {}: {}".format(name, tp))
		self.logger.deepdebug(" > reading chunk: DONE" )
	def __raw_stream__(self):
		for name in self:
			for chunkid,(fl,st,sz,tp)in enumerate(self.datamap[name]):
				with open(self.filename if fl is None else fl,"rb") as fd:
					fd.seek(st)
					data = fd.read(sz)
				yield name,chunkid,sz,tp,data
	def __stream__(self):
		for name in self:
			for chunkid,chunk in enumerate(self.datamap[name]):
				yield name,chunkid,self.__read_chunk__(*chunk)
	def __gen__(self,name):
		if not name in self.datamap:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in __gen__")
			self.logger.error(" Cannot find  record {}".format(name))
			self.logger.error("----------------------------------------------------")
			raise RuntimeError("Cannot find  record {}".format(name))
		for fl, st, sz, tp in self.datamap[name]:
			yield self.__read_chunk__(fl,st,sz,tp)
		for dname,ddata in self.bufdata:
			if dname == name: yield ddata
	def __getitem__(self,key):
		"""
		Possible syntaxes:
		for n in data         -> generator for all names in data file
		data[None]            -> returns a generator of name,chunk_number,chunk_data for all data in the file
		data[None,None]       -> returns a generator for raw data strim:
								  name,chunk_number,data_size,data_type,data for all data in the file.
								  it can be used for transferring data to a server or so on. 
		data['/name']         -> returns a generator for all chunks of data under /name
		data['/name',2]       -> returns 3rd chunk of the /name's data
		data['/name',2,5,7]   -> returns list of chunks of the /name's data
		data['/name',None]    -> returns a list with all chunks of /name's data
		data['/name',]        -> returns total number of chunks for a /name
		data['/name',(3,7,2)] -> returns a list of data within a slice (3,7,2)
		"""
		if (not self.filename is None) and self.mtime != os.stat(self.filename).st_mtime: self.readfooter()
		if   key is None                              : return self.__stream__()
		elif type(key) is str or type(key) is unicode : return self.__gen__(key)
		elif type(key) is tuple:
			name = key[0]
			if not name in self.datamap:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in get")
				self.logger.error(" Cannot find  record {}".format(name))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Cannot find  record {}".format(name))
			if   len(key) == 1 : return len(self.datamap[name])
			elif len(key) >= 2 :
				ret = []
				for chunk in key[1:]:
					if key is None and chunk is None   : return self.__raw_stream__()
					if chunk is None                   : 
						for fl, st, sz, tp in self.datamap[name]:
							ret.append( self.__read_chunk__(fl,st,sz,tp) )
					elif type(chunk) is int            :
						if abs(chunk) >= len(self.datamap[name]):
							self.logger.error("----------------------------------------------------")
							self.logger.error(" DATA ERROR in get")
							self.logger.error(" Chunk {} greater than record {} size {}".format(chunk, name, len(self.datamap[name])))
							self.logger.error("----------------------------------------------------")
							raise RuntimeError("Chunk {} greater than record {} size {}".format(chunk, name, len(self.datamap[name])))
						if len(key) == 2:
							return self.__read_chunk__( *self.datamap[name][chunk] )
						else:
							ret.append( self.__read_chunk__(fl,st,sz,tp) )
					elif type(chunk) is tuple          :
						sl = slice(*chunk)
						for fl, st, sz, tp in self.datamap[name][sl]:
							ret.append( self.__read_chunk__(fl,st,sz,tp) )
					else:
						self.logger.error("----------------------------------------------------")
						self.logger.error(" DATA ERROR in get")
						self.logger.error(" Incorrect chunk type for name {}. It should int or tuple, {} given".format(name, type(chunk)))
						self.logger.error("----------------------------------------------------")
						raise RuntimeError("Incorrect chunk type for name {}. It should int or tuple, {} given".format(name, type(chunk)))
				for dkey,ddata in self.bufdata:
					if dkey == key: ret.append(ddata)
				return ret
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in get")
				self.logger.error(" Unexpected error with key{}".format(key))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Unexpected error with key{}".format(key))
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in get")
			self.logger.error(" Incorrect type of key {}.".format(key))
			self.logger.error("----------------------------------------------------")
			raise RuntimeError("Incorrect type of key {}.".format(key))
	def aggregate(self,*stkdatafiles):
		for f in stkdatafiles:
			if isinstance(f,data):
				if f.filename == self.filename and self.filename is not None: continue
				for name in f:
					if not name in self.datamap: self.datamap[name]=[]
					self.datamap[name] += [ (f.filename if fl is None else fl,st,sz,tp) for fl,st,sz,tp in f.datamap[name] if fl != self.filename or fl is None ]
			elif type(f) is str or type(f) is unicode:
				with data(f) as tsd:
					for name in tsd:
						if not name in self.datamap: self.datamap[name]=[]
						self.datamap[name] += [ (tsd.filename if fl is None else fl,st,sz,tp) for fl,st,sz,tp in tsd.datamap[name] if fl != self.filename or fl is None ]
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in aggregate")
				self.logger.error(" Incorrect type of file {}.".format(f))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Incorrect type of file {}.".format(f))
		self.writefooter()
				

	def __contains__(self,key): return key in self.datamap
	def __iter__(self):
		for name in self.datamap       : yield name
	def dict(self):
		for name in self.datamap.dict(): yield name
	def __delitem__(self,key):
		if type(key) is str or type(key) is unicode :
			del self.datamap[key]
		elif type(key) is tuple:
			name = key[0]
			if not name in self.datamap:
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in __delitem__")
				self.logger.error(" Cannot find  record {}".format(name))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Cannot find  record {}".format(name))
			if   len(key) == 1 : return self.__delitem__(name)
			elif len(key) == 2 :
				chunk = key[1]
				if type(chunk) is int:
					self.datamap[name] = self.datamap[name][:chunk]+self.datamap[name][chunk+1:]
				elif type(chunk) is tuple:
					sl = slice(*chunk)
					del self.datamap[name][sl]
			else :
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in __delitem__")
				self.logger.error(" Too many chunks to delete in {}, use one chunk at the time or slice notation".format(name))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Too many chunks to delete in {}, use one chunk at the time or slice notation".format(name))
		if self.autodefragmentation: self.defragmentation()
	def defragmentation(self): 
		"""
		it goes over all names and check to find gaps in data positions
		after deletion. If there are gaps, it should move data and reset 
		file size.
		"""

		reclist  = [ st for nm in self for fl,st,sz,tp in self.datamap[nm] ]
		reclist.sort()
		
		with open(self.filename,"rw+b") as fd:
			mm = mmap.mmap(fd.fileno(), 0)
			l = len(mm)
			p,r,c = 0,0,0
			while p < l:
				while mm[p:p+8] !='#STKDATA' and p < l: p += 1
				if p >= l: continue
				if p != r:
					mm[r:-p+r] = mm[p:]
					c += p-r
					p = r
				chheadersize, = struct.unpack(">H",mm[p+8:p+10])
				sz,ch,ty,name = eval(mm[p+10:p+10+chheadersize])
				df = p+10+chheadersize
				if df+c in reclist:
					if p != r:
						mm[r:-p+r] = mm[p:]
						c += p-r
						p = r
					r = p = p+10+chheadersize+sz
					continue
				else:
					p = df+sz					
		self.datamap = {}	
		self.rescan_file()
		
	#--- TODO ---#		
	def __call__(self,name,*key):
		"""
		Possible syntaxes:
		data(None)            -> 
		data(None,None)       -> 
		data('/name')         -> returns a concatenation of all data under /name 
		data('/name',2)       -> 
		data('/name',2,5,7)   -> returns a concatenation of chunks 2, 5 and 7 of /name 
		data('/name',None)    -> 
		data('/name',)        -> 
		data('/name',(3,7,2)) -> returns a concatenation of data in slice (3,7,2) for /name's chunks 
		"""
		if (not self.filename is None) and self.mtime != os.stat(self.filename).st_mtime: self.readfooter()
		pass
	#-------------#
	
	#### Parallel Functions ####
	def p_send2pipe(self):
		if not self.dthread.is_alive() :
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in pset")
			self.logger.error(" Receiver DEAD!")
			self.logger.error("----------------------------------------------------")
			raise RuntimeError("Receiver DEAD!")
		self.logger.deepdebug(" > Sender: Sending into the pipe  {} ... ".format([n for n,d in self.bufdata]) )
		self.queue.put( self.bufdata )
		self.logger.deepdebug(" > Sender: Cleaning memory " )
		self.bufdata = []
		self.logger.deepdebug(" > Sender: len(bufdata)={}, Queue.qsize()={}".format(len(self.bufdata),self.queue.qsize()) )
		
	def p__exit__(self, exc_type, exc_value, traceback):
		self.sync()
		self.logger.deepdebug(" > Sender: Termination")
		self.queue.put([])
		self.logger.deepdebug(" > Sender: Wating to join ... ")
		self.dthread.join()
		self.logger.deepdebug(" > Sender: Receiver has joined ")
		
	def pset(self,name,data):
		self.bufdata.append( (name,data) )
		if len(self.bufdata) >= self.parallel*4:
			self.p_send2pipe()
			self.logger.deepdebug(" > Sender: Memory size: {} - {}".format(self.process.memory_info().rss,self.maxbufsize) )

			
	def psync(self):
		if len(self.bufdata) > 0: self.p_send2pipe()
		self.logger.deepdebug(" > Synch  : Sendding synch " )
		self.queue.put( [[]] )
		if not self.dthread.is_alive() : 
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in psync")
			self.logger.error(" Receiver DEAD!")
			self.logger.error("----------------------------------------------------")
			raise RuntimeError("Receiver DEAD!")
			
	def runzippers(self):
		if not self.sthread.is_alive():
			self.sthread = threading.Thread(target=self.savebuffed)
			self.sthread.start()
		threading.Timer(3.141, self.runzippers).start()
		self.pids = [ pid for pid in self.pids if pid.is_alive() ]
		if len(self.prebuff) > 0: return
		if len(self.pids) > self.parallel :
			self.logger.deepdebug(" < Receiver: number of working threads = {} ".format(len(self.pids)) )
			return
		needthreads = self.parallel-len(self.pids)
		needthreads = needthreads if needthreads < len(self.prebuff) else len(self.prebuff) 
		pids = [ threading.Thread(target=self.pzipper, args=()) for i in xrange(needthreads) ]
		for pid in pids: pid.start()
		self.pids += pids
		self.logger.deepdebug(" < Receiver: number of working threads = {} ".format(len(self.pids)) )
		
	
	def pzipper(self):
		while len(self.prebuff) > 0:
			ditem = None
			with self.lock:
				if len(self.prebuff) > 0:
					ditem = self.prebuff[0]
					self.prebuff = self.prebuff[1:]
			if ditem is None:
				time.sleep(1)
				continue
			self.logger.deepdebug(" < Zipper  : took {} for zipping ...".format(ditem[0]) )
			zippedditem = self.zipper( *ditem )
			with self.lock:
				self.bufdata.append(zippedditem)
			self.logger.deepdebug(" < Zipper  : {} has been zipped and ready to save".format(ditem[0]) )
		
		

	def savebuffer(self):
		"""
		savebuffer is a process which receives data from the pipe and collects it in memory (bufdata).
		It runs a thread, which reads data form bufdata and saves recordings on disk
		"""
		self.bufdata = []
		self.prebuff = [] #raw data from pipe
		self.sthread = threading.Thread(target=self.savebuffed)
		self.lock    = threading.Lock()
		self.pids    = []
		self.runzippers() #run timer-ed update of threads
		
		
		self.process = psutil.Process()
		self.logger.deepdebug(" < Receiver: pid={}".format(self.process.pid) )
		if self.maxbufsize < 1:
			self.maxbufsize = psutil.virtual_memory().total/4
			self.logger.deepdebug(" < Receiver: maxbuffersize={}".format(self.maxbufsize))
		while 1:
			dbuf = self.queue.get()
			if len(dbuf) == 0:
				while len(self.prebuff) !=0 :
					time.sleep(1)
				for pid in self.pids: pid.join()
				self.pids = []
				if self.sthread.is_alive():self.sthread.join()
				self.savebuffed()
				self.writefooter()
				self.logger.deepdebug(" < Receiver: TERMINATION " )
				return
			elif len(dbuf) == 1 and len(dbuf[0]) == 0:
				while len(self.prebuff) !=0 :
					time.sleep(1)
				for pid in self.pids: pid.join()
				self.pids = []
				if self.sthread.is_alive():self.sthread.join()
				self.savebuffed()
				self.writefooter()
				self.logger.deepdebug(" < Receiver: Synchronization " )
			else:
				self.logger.deepdebug(" < Receiver: get a data({}): {}".format(len(dbuf),[n for n,d in dbuf]) )
				if self.lock.acquire(False):
					self.prebuff += dbuf
					self.lock.release()
	def savebuffed(self):
		while len(self.bufdata) != 0:
			with self.lock:
				name,data,datatype = self.bufdata[0]
				self.bufdata=self.bufdata[1:]
			self.__save_chunk__( name,data,datatype )
			self.logger.deepdebug(" < Receiver: Chunk {} saved, data removed ".format(name) )



if __name__ == "__main__":
	logging.basicConfig(format='%(asctime)s:%(name)-33s%(lineno)-6d%(levelname)-8s:%(message)s', level=logging.DEEPDEBUG)
	
	if len(sys.argv) == 3:
		with data(sys.argv[1]) as sd1,data(sys.argv[2]) as sd2:
			print "BEFORE ADDING"
			print " FILE 1:"
			print " Data length", len(sd1)
			for name in sd1:
				print " number of chunks in name",name,"=",sd1[name,]
			print
			print " FILE 2:"
			print " Data length", len(sd2)
			for name in sd2:
				print " number of chunks in name",name,"=",sd2[name,]
			print
			print "=====================================================\n"
			print "SUMMING File 1 and File 2"
			nsd = sd1 + sd2
			print " FILE",nsd.filename
			print " Data length", len(nsd)
			for name in nsd:
				print " number of chunks in name",name,"=",nsd[name,]
			print
			print " FILE 1:"
			print " Data length", len(sd1)
			for name in sd1:
				print " number of chunks in name",name,"=",sd1[name,]
			print
			print " FILE 2:"
			print " Data length", len(sd2)
			for name in sd2:
				print " number of chunks in name",name,"=",sd2[name,]
			print
			print "=====================================================\n"
			print "AGGREGATING File 2 into File 1:"
			#>>aggregate another file.....
			sd1 += sd2
			print "AFTER IN-PLACE ADDITION ANOTHER FILE"
			print " FILE 1:"
			print "Data length", len(sd1)
			for name in sd1:
				print "number of chunks in name",name,"=",sd1[name,]
			print
			print " FILE 2:"
			print " Data length", len(sd2)
			for name in sd2:
				print " number of chunks in name",name,"=",sd2[name,]
			print
			print "=====================================================\n"
			print "#DB>>"
			print "#DB>> TREE:"
			for n in sd1.datamap	:
				for i,p in enumerate(sd1.datamap[n]):
					print "#DB>>   ",n,"[%02d]="%i,p
		print 
		print "CHECK one more time"
		with data(sys.argv[1]) as sd:
			print "Data length", len(sd)
			for name in sd:
				print "number of chunks in name",name,"=",sd[name,]
		for n,i,d in data(sys.argv[1])[None]:
			print "DATACHECK> ",n,"[%03d]="%i,d
	elif len(sys.argv) == 2:
		print "#DB>> st"
		with data(sys.argv[1],autocorrection=True) as sd:
			print "#DB>> in"
			sd["/np/array"]=np.random.rand(50) 
			sd["/x/np/array"]=np.random.rand(70)
			print "#DB>> TREE:"
			for n in sd.datamap	:
				for i,p in enumerate(sd.datamap[n]):
					print " ",n,"[%02d]="%i,p
			print "Data length", len(sd)
			for name in sd:
				print "number of chunks in name",name,"=",sd[name,]
		print "#DB>> out"
		#exit(0)
		with data(sys.argv[1],compress=False) as sd:
			sd["/prime"]="number"
			sd["/simple"]="words"

		print "data[\"/prime\"]       =",data(sys.argv[1])["/prime"]
		print "data[\"/simple\"]      =",data(sys.argv[1])["/simple"]
		print "data[\"/prime\",None]  =",data(sys.argv[1])["/prime",None]
		print "data[\"/simple\",None] =",data(sys.argv[1])["/simple",None]
		
		with data(sys.argv[1]) as sd:
			sd["/prime"]=(1,2,3,5)


		print "print      data[\"/np/array\"]      =",     data(sys.argv[1])["/np/array"]
		print "print type(data[\"/np/array\"]     )=",type(data(sys.argv[1])["/np/array"])
		print "print      data[\"/np/array\",None] =",     data(sys.argv[1])["/np/array",None]
		print "print type(data[\"/np/array\",None])=",type(data(sys.argv[1])["/np/array",None])
		print "print type(data[\"/np/array\",0]   )=",type(data(sys.argv[1])["/np/array",0])
		print "print data[\"/np/array\",0].shape   =",     data(sys.argv[1])["/np/array",0].shape
		
		
		for n,i,d in data(sys.argv[1])[None]:
			print "DATACHECK> ",n,"[%03d]="%i,d

		print "negative chunk", data(sys.argv[1])["/x/np/array",-1]

		
		#print "positive oversize", data(sys.argv[1])["/x/np/array", 1000]
		#print "negative oversize", data(sys.argv[1])["/x/np/array",-1000]
