import os, sys, types, logging, hashlib, zlib, threading, io, time, struct, psutil
from multiprocessing import Process, Queue 
import mmap
import numpy as np

from simtoolkit.tree import tree

logging.DEEPDEBUG = 5
logging.addLevelName(logging.DEEPDEBUG, "DEEPDEBUG")
logging.Logger.deepdebug = lambda inst, msg, *args, **kwargs: inst.log(logging.DEEPDEBUG, msg, *args, **kwargs)
logging.deepdebug = logging.Logger.deepdebug

class data:
	def __init__(self, filename, mode="r+", compress = 5, npcompress=False, parallel=False, maxbuffersize=0, autocorrection=False):
		self.logger = logging.getLogger("simtoolkit.data")
		self.logger.deepdebug(" > Open simdata: file={}, modw={}, compress={}, parallel={}, npcompress={}, maxbuffersize={}".format(filename, mode, compress, parallel, npcompress,maxbuffersize))
		self.filename = filename
		self.autocorrection = autocorrection
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
			self.maxbufsize = psutil.virtual_memory().total/4
			self.logger.deepdebug(" < Receiver: maxbuffersize={}".format(self.maxbufsize))
		if self.parallel:
			self.queue   = Queue(maxsize = self.maxbufsize/10)
			self.dthread = Process(target=self.savebuffer) 
			self.dthread.start()
			self.process = psutil.Process()
			self.__exit__ = self.p__exit__
			self.set      = self.pset
			self.sync     = self.psync
		else:
			self.__exit__ = self.s__exit__
			self.set      = self.sset
			self.sync     = self.ssync
		

	def initfooter(self):
		self.datamap = tree()
		self.mtime   = time.time()
		self.tail    = 0
		self.fsize   = 0
		if self.filename is None: return
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
					self.datamap = tree().imp( eval(zlib.decompress(fd.read(idx)) ) )
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
				if self.tail <= st+sz: self.tail = st+sz+1
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
				self.tail = start+10+chheadersize+sz+1
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
		
				
		"TODO: needs to scan a file, find all headers and for all records and restore footer"
		pass

	def writefooter(self):
		if self.filename is None:
			self.logger.warning("----------------------------------------------------")
			self.logger.warning(" DATA ERROR in writefooter")
			self.logger.warning(" Cannot write footer into virtual file")
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
			self.tail += 10 + len(chheader) + datasize +1 #? <<
		return self
	def __enter__(self)                                : return self
	def __setitem__(self,key,value)                    : self.set(key,value)
	def __len__(self) :
		cnt=0
		for n in self: cnt += 1
		return cnt
	def __add__(self,xdata):
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
					if chunk is None                    : 
						for fl, st, sz, tp in self.datamap[name]:
							ret.append( self.__read_chunk__(fl,st,sz,tp) )
					elif type(chunk) is int             :
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
					elif type(chunk) is tuple           :
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
	def __call__(self,name,*key):
		"""
		Possible syntaxes:
		data(None)            -> 
		data(None,None)       -> 
		data('/name')         -> returns a concatenation of all data under /name 
		data('/name',2)       -> 
		data('/name',2,5,7]   -> returns a concatenation of chunks 2, 5 and 7 of /name 
		data('/name',None]    -> 
		data('/name',]        -> 
		data('/name',(3,7,2)] -> returns a concatenation of data in slice (3,7,2) for /name's chunks 
		"""
		if (not self.filename is None) and self.mtime != os.stat(self.filename).st_mtime: self.readfooter()
		pass
	def aggregate(self,*stkdatafiles):		
		for f in stkdatafiles:
			if isinstance(f,data):
				if f.filename == self.filename : continue
				for name in f:
					if not name in self.datamap: self.datamap[name]=[]
					self.datamap[name] += [ (f.filename if fl is None else fl,st,sz,tp) for fl,st,sz,tp in f.datamap[name] if fl != self.filename ]
			elif type(f) is str or type(f) is unicode:
				with data(f) as tsd:
					for name in tsd:
						if not name in self.datamap: self.datamap[name]=[]
						self.datamap[name] += [ (tsd.filename if fl is None else fl,st,sz,tp) for fl,st,sz,tp in tsd.datamap[name] if fl != self.filename ]
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


	#### Parallel Functions ####
	def p__exit__(self, exc_type, exc_value, traceback):
		self.sync()
		self.logger.deepdebug(" > Sender: Termination")
		self.queue.put([])
		self.logger.deepdebug(" > Sender: Wating to join ... ")
		self.dthread.join()
		self.logger.deepdebug(" > Sender: Receiver has joined ")
		
	def pset(self,name,data):
		self.bufdata.append( (name,data) )
		#if self.process.memory_info().rss >= self.maxbufsize/100:
		if len(self.bufdata) >= self.parallel:
			if not self.dthread.is_alive() :
				self.logger.error("----------------------------------------------------")
				self.logger.error(" DATA ERROR in pset")
				self.logger.error(" Receiver DEAD!")
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Receiver DEAD!")
			self.logger.deepdebug(" > Sender: ZIPPPPING.....")
			#pids = [ threading.Thread(target=self.pzipper, args=(idx,)) for idx in xrange(self.parallel) ]
			for pload in xrange(0,len(self.bufdata),self.parallel):
				lastid = pload+self.parallel if pload+self.parallel < len(self.bufdata) else len(self.bufdata)
				self.logger.deepdebug(" > Sender: ZIPPING {}-{}".format(pload,lastid))
				pids = [ threading.Thread(target=self.pzipper, args=(idx,)) for idx in xrange(pload,lastid) ]
				for pid in pids: pid.start()
				for pid in pids: pid.join()
			self.logger.deepdebug(" > Sender: Sendding into pipe ... ")
			self.queue.put( self.bufdata )
			self.logger.deepdebug(" > Sender: Cleaning memory " )
			self.bufdata = []
			
	def psync(self):
		self.logger.deepdebug(" > Synch: ZIPPPPING.....")
		pids = [ threading.Thread(target=self.pzipper, args=(idx,)) for idx in xrange( len(self.bufdata) ) ]
		for pid in pids: pid.start()
		for pid in pids: pid.join()
		self.logger.deepdebug(" > Synch: Sendding into pipe ... ")
		self.queue.put( self.bufdata )
		self.logger.deepdebug(" > Synch: Cleaning memory " )
		self.bufdata = []
		self.logger.deepdebug(" > Synch: Sendding synch " )
		self.queue.put( [[]] )
		if not self.dthread.is_alive() : 
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in psync")
			self.logger.error(" Receiver DEAD!")
			self.logger.error("----------------------------------------------------")
			raise RuntimeError("Receiver DEAD!")
			
	def pzipper(self,idx):
		self.bufdata[idx] = self.zipper( *self.bufdata[idx] )
		#self.logger.deepdebug(" < Receiver: zipped data len {} ".format(len(self.bufdata[idx])) )

	def savebuffer(self):
		"""
		savebuffer is a process which receives data from the pipe and collects it in memory (bufdata).
		It runs a thread, which reads data form bufdata and saves recordings on disk
		"""
		self.bufdata = []
		self.sthread = threading.Thread(target=self.savebuffed)
		self.process = psutil.Process()
		if self.maxbufsize < 1:
			self.maxbufsize = psutil.virtual_memory().total/4
			self.logger.deepdebug(" < Receiver: maxbuffersize={}".format(self.maxbufsize))
		while 1:
			dbuf = self.queue.get()
			if len(dbuf) == 0:
				if self.sthread.is_alive():self.sthread.join()
				self.savebuffed()
				self.writefooter()
				self.logger.deepdebug(" < Receiver: TERMINATION " )
				return
			elif len(dbuf) == 1 and len(dbuf[0]) == 0:
				if self.sthread.is_alive():self.sthread.join()
				self.savebuffed()
				self.writefooter()
				self.logger.deepdebug(" < Receiver: Synchronization " )
			else:
				#sys.stderr.write("Receive {} {}\r".format(dbuf[0][0],dbuf[-1][0]))
				#TODO >>if self.sthread.is_alive(): 	self.sthread.join()
				#B.nbytes
				self.bufdata += dbuf
				self.logger.deepdebug(" < Receiver: Check memory overflow .... " )
				while self.process.memory_info().rss >= self.maxbufsize:
					if not self.sthread.is_alive():
						self.sthread = threading.Thread(target=self.savebuffed)
						self.sthread.start()
					time.sleep(1)
				self.logger.deepdebug(" < Receiver: Check point has been passed " )
			if self.process.memory_info().rss >= self.maxbufsize/100:
				if not self.sthread.is_alive():
					self.sthread = threading.Thread(target=self.savebuffed)
					self.sthread.start()
	def savebuffed(self):
		while len(self.bufdata) != 0:
			name,data,datatype = self.bufdata[0]
			self.__save_chunk__( name,data,datatype )
			self.bufdata=self.bufdata[1:]	



if __name__ == "__main__":
	#logging.basicConfig(format='%(asctime)s:%(name)-33s%(lineno)-6d%(levelname)-8s:%(message)s', level=logging.DEEPDEBUG)
	
	if len(sys.argv) > 2:
		with data(sys.argv[1]) as sd:
			print "BEFORE ADDING ANOTHER FILE"
			print "Data length", len(sd)
			for name in sd:
				print "number of chunks in name",name,"=",sd[name,]
			print
			
			#>>aggregate another file.....
			sd += data(sys.argv[2])
			print "AFTER ADDING ANOTHER FILE"
			print "Data length", len(sd)
			for name in sd:
				print "number of chunks in name",name,"=",sd[name,]
			print
			print "#DB>>"
			print "#DB>> TREE:"
			for n in sd.datamap	:
				for i,p in enumerate(sd.datamap[n]):
					print "#DB>>   ",n,"[%02d]="%i,p
		print 
		print "CHECK one more time"
		with data(sys.argv[1]) as sd:
			print "Data length", len(sd)
			for name in sd:
				print "number of chunks in name",name,"=",sd[name,]
		for n,i,d in data(sys.argv[1])[None]:
			print "DATACHECK> ",n,"[%03d]="%i,d
	else:
		print "#DB>> st"
		with data(sys.argv[1],autocorrection=True) as sd:
			print "#DB>> in"
			sd.set("/np/array",np.random.rand(50) )
			sd.set("/x/np/array",np.random.rand(70) )
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
			sd.set("/prime","number").set("/simple","words")

		print "data[\"/prime\"]       =",data(sys.argv[1])["/prime"]
		print "data[\"/simple\"]      =",data(sys.argv[1])["/simple"]
		print "data[\"/prime\",None]  =",data(sys.argv[1])["/prime",None]
		print "data[\"/simple\",None] =",data(sys.argv[1])["/simple",None]
		
		with data(sys.argv[1]) as sd:
			sd.set("/prime",(1,2,3,5))


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
