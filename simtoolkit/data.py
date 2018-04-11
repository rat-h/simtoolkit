import os, sys, types, logging, hashlib, zlib, threading, io, time, struct, psutil
from multiprocessing import Process, Queue 

from numpy import *

from simtoolkit.tree import tree

logging.DEEPDEBUG = 5
logging.addLevelName(logging.DEEPDEBUG, "DEEPDEBUG")
logging.Logger.deepdebug = lambda inst, msg, *args, **kwargs: inst.log(logging.DEEPDEBUG, msg, *args, **kwargs)
logging.deepdebug = logging.Logger.deepdebug

class data:
	def __init__(self, filename, mode="r+", compress = 5, npcompress=False, parallel=False, maxbuffersize=0, repare=False):
		self.logger = logging.getLogger("simtoolkit.data")
		self.logger.deepdebug(" > Open simdata: file={}, modw={}, compress={}, parallel={}, npcompress={}, maxbuffersize={}".format(filename, mode, compress, parallel, npcompress,maxbuffersize))
		self.filename = filename
		if mode != "w":
			self.readfooter()
		else:
			self.initfooter()

		self.bufdata = []
		self.compres = compress
		self.parallel = parallel
		self.npcompress = npcompress
		self.maxbufsize = maxbuffersize
		self.repare = repare
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
					idx =  struct.unpack(">Q",fd.read(8))[0] #Tree size
					fd.seek(-idx-8,2)
					#importing back to the tree
					self.datamap = tree().imp( eval(zlib.decompress(fd.read(idx)) ) )
			except BaseException as e:
				self.logger.warning("----------------------------------------------------")
				self.logger.warning(" DATA ERROR in readfooter")
				self.logger.warning(" Cannot open file \'{}\': {}".format(self.filename,e))
				self.logger.warning("----------------------------------------------------")		
				if not self.repare:
					raise RuntimeError("Cannot open file \'{}\': {}".format(self.filename,e))
				else:
					self.repare_file()
				#self.initfooter()
		else:
			self.initfooter()
		self.mtime = os.stat(self.filename).st_mtime
		self.tail  = 0
		for n in self.datamap:
			fl,st,sz,tp = self.datamap[n][-1]
			if not fl is None: continue
			if self.tail <= st+sz: self.tail = st+sz+1

	def repare_file(self):
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
			fd.seek( self.tail )
			footer = zlib.compress(str(self.datamap.exp()),9)
			fd.write(footer)
			fd.write(struct.pack(">Q",len(footer)) )
			
			
		self.mtime = os.stat(self.filename).st_mtime
	

	def zipper(self, name, data):
		if type(data) is str or type(data) is unicode:
			if self.compres:
				return name, zlib.compress(data,self.compres),"ZSTRING"
			else:
				return name, data,"STRING"	
		elif not isinstance(data,ndarray):
			if self.compres:
				return name, zlib.compress(str(data),self.compres),"ZPYTHON"
			else:
				return name, str(data),"PYTHON"
		else:
			with io.BytesIO() as fd:
				save(fd, data)
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
			self.tail += 10 + len(chheader) + datasize #+1 ? <<
		return self
	def __enter__(self)                                : return self
	def s__exit__(self, exc_type, exc_value, traceback): self.writefooter()
	def __setitem__(self,key,value)                    : self.set(key,value)
	def sset(self,name,data)                           : return self.__save_chunk__( *self.zipper( name, data ) )
	def ssync(self)                                    : self.writefooter()
	def __getitem__(self,key): 
		if key is None: return self.dict()
		return self.get(key)
	def __read_chunk__(self,fl,st,sz,tp):
		with open(self.filename if fl is None else fl,"rb") as fd:
			fd.seek(st)
			if   tp == "ZNUMPY":
				return load(io.BytesIO(zlib.decompress(fd.read(sz)))) 
			elif tp == "NUMPY":
				return  load(io.BytesIO(fd.read(sz)))
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
				self.logger.error(" Unsopported data format for name {}: {}".format(name, tp))
				self.logger.error("----------------------------------------------------")
				raise RuntimeError("Unsopported data format for name {}: {}".format(name, tp))
	def get(self, name):
		if (not self.filename is None) and self.mtime != os.stat(self.filename).st_mtime: self.readfooter()
		#if type(name) is tuple:
		#	name,chunk = name
		if not name in self.datamap:
			self.logger.error("----------------------------------------------------")
			self.logger.error(" DATA ERROR in get")
			self.logger.error(" Cannot find  record {}".format(name))
			self.logger.error("----------------------------------------------------")
			raise RuntimeError("Cannot find  record {}".format(name))
		ret = []
		for fl, st, sz, tp in self.datamap[name]:
			ret.append( self.__read_chunk__(fl,st,sz,tp) )
		for dname,ddata in self.bufdata:
			if dname == name: ret.append(ddata)
		return ret

	def aggregate(self,*sdkdatafiles):
		for f in stkdatafiles:
			with data(f) as tsd:
				for name in tsd:
					if not name in self.datamap: self.datamap[name]=[]
					self.datamap[name] += [ (f,st,sz,tp) for fl,st,sz,tp in tsd.daamap[name] ]

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
	print "#DB>> st"
	with data(sys.argv[1]) as sd:
		print "#DB>> in"
		sd.set("/np/array",random.rand(50) )
		sd.set("/x/np/array",random.rand(70) )
		print "#DB>> TREE:",sd.datamap		
	print "#DB>> out"
	
	with data(sys.argv[1],compress=False) as sd:
		sd.set("/prime","number").set("/simple","words")

	print "data[\"/prime\"]    =",data(sys.argv[1])["/prime"]
	print "data.get(\"/simple\")=",data(sys.argv[1]).get("/simple")
	
	with data(sys.argv[1]) as sd:
		sd.set("/prime",(1,2,3,5))

	print "data[\"/prime\"]    =",data(sys.argv[1])["/prime"]
	#print data(sys.argv[1]).get("/prime",1)
	print data(sys.argv[1]).get("/simple")

	print "print      data[\"/np/array\"]    =",     data(sys.argv[1]).get("/np/array")
	print "print type(data[\"/np/array\"]   )=",type(data(sys.argv[1]).get("/np/array"))
	print "print type(data[\"/np/array\"][0])=",type(data(sys.argv[1]).get("/np/array")[0])
	print "print data[\"/np/array\"][0].shape=",     data(sys.argv[1]).get("/np/array")[0].shape
	
	print data(sys.argv[1]).get("/x/np/array")
	print type(data(sys.argv[1]).get("/x/np/array"))
	print type(data(sys.argv[1]).get("/x/np/array")[0])
	print data(sys.argv[1]).get("/x/np/array")[0].shape

	with data(sys.argv[1]) as sd:
		for name in sd:
			print name, sd[name]

