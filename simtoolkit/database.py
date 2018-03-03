import sys, zlib, os, platform, logging, hashlib, io
from datetime import datetime
from random import randint
import sqlite3
from urlparse import urlparse
from simtoolkit.tree import tree
import numpy  as np

class db:
	def __init__(self, dburl, mode="", username="", password="", architecture=platform.machine() ):
		"""
		db is an aggregator for sevral possible data bases and format versions:
		
		"""
		self.logger = logging.getLogger("simtoolkit.database.db")
		self.dburl = dburl
		self.mode  = "wr"
		up = urlparse(dburl)
		self.dbtype = "file" if up.scheme == "" else up.scheme
		if up.query != "":
			upq = dist( urlparse.parse_qsl(up.query) )
			if "mode" in upq: self.mode = upq['mode']
		self.path     = dburl if up.path == "" else up.path
		self.username = up.username
		self.password = up.password
		
		if mode     != "": self.mode     = mode
		if username != "": self.username = username
		if password != "": self.password = password
		
		#Default values
		if self.dbtype == "" : self.dbtype = "file"

		if self.dbtype == "file":
			if   self.mode == "wr" or self.mode == "rw":
				self.db = sqlite(self.path, architecture=architecture)
			elif self.mode == "w":
				with open(self.path,"w") as fd: pass
				self.db = sqlite(self.path, architecture=architecture)
			elif self.mode == "ro":
				self.logger.warning(" > read-only mode is not supported for file database. Open {} in RW mode".format(self.path) )
				self.db = sqlite(self.path, architecture=architecture)
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(db.__init__)")
				self.logger.error("          : Unknown mode {}".format(self.mode))
				self.logger.error("----------------------------------------------------")		
				raise ValueError("Unknown mode {}".format(self.mode))
		#elif self.dbtype == "mysql"
		#elif self.dbtype == "postgresql"
		#elif self.dbtype == "oracle"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(db.__init__)")
			self.logger.error("          : Data base connector for {} isn't implemented yet".format(self.dbtype))
			self.logger.error("----------------------------------------------------")		
			raise ValueError("Data base connector for {} isn't implemented yet".format(self.dbtype))
		#Redirection to the database
		#---   Itergator   ---
		self.__iter__     = self.db.__iter__
		self.poolrecs     = self.db.poolrecs
		self.poolnames    = self.db.poolnames
		#--- RAW interface ---
		self.recs         = self.db.recs
		self.names        = self.db.names
		#--
		self.__delitem__  = self.db.__delitem__
		#---     TAGS      ---
		self.settag       = self.db.settag
		self.rmtag        = self.db.rmtag
		self.pooltags     = self.db.pooltags
		self.tags         = self.db.tags
		#---  Information   ---
		self.info         = self.db.info
	def packvalue(self,name,value):
		if type(value) is str or type(value) is unicode:
			return 'ZIP',buffer(zlib.compress(value,9))
		elif isinstance(value,np.ndarray):
			with io.BytesIO() as fd:
				np.save(fd,value)
				return 'ZIPNUMPY',buffer(zlib.compress(fd.getvalue(),9))
		else:
			return 'ZIP',buffer(zlib.compress("{}".format(value),9))
	def unpackvalue(self,name,valtype,value):
		if   valtype == "ZIP":      return zlib.decompress(value)
		elif valtype == "ZIPNUMPY": return np.load(io.BytesIO(zlib.decompress(value)))
		elif valtype == "NUMPY":    return np.load(io.BytesIO(value))
		elif valtype == "TEXT":     return value
		else:
			logger.error("----------------------------------------------------")
			logger.error("SimToolKit: DATABASE ERROR(db.uppackvalue)")
			logger.error("          : Unknown data type {} of parameter {}".format(valtype,name))
			logger.error("----------------------------------------------------")		
			raise RuntimeError("Unknown data type {} of parameter {}".format(valtype,name))

	def record(self, tree, message, rechash=None, timestamp=None):
		if self.mode == "ro":
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(db.record)")
			self.logger.error("          : Cannot record in read-only data base")
			self.logger.error("----------------------------------------------------")		
			raise ValueError("Cannot record in read-only data base")
			
		if rechash is None or rechash == "" :
			h = hashlib.sha1()
			for n in tree:
				h.update(str(tree[n]))
			rechash = h.hexdigest()
		if timestamp is None or timestamp == "" :
			now = datetime.now()
			timestamp = "%d-%d-%d %d:%d:%d.%d"%(now.year, now.month, now.day, now.hour, now.minute, now.second, randint(0,999))
		for n in tree:
			tree[n] = self.packvalue(n,tree[n])
		return self.db.record(tree, timestamp, rechash, message)
	def values(self,flt=None,column=None):
		for valid,record,name,valtype,value in self.db.values(flt=flt,column=column): yield valid,record,name,self.unpackvalue("RAW"+valtype,valtype,value)
	def __setitem__(self, key, value):
		if isinstance(value,tree):
			for n in value:
				value[n] = self.packvalue(n,value[n])
		else:
			value = self.packvalue(key,value)
		self.db.__setitem__(key,value)
	def __getitem__(self, key):
		Atree = self.db[key]
		if Atree is None:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(db.__getitem__)")
			self.logger.error("          : Cannot find hash or timestamp {} in database {}".format(key,self.dburl))
			self.logger.error("----------------------------------------------------")		
			raise ValueError("Cannot find hash or timestamp {} in database {}".format(key,self.dburl))
		for n in Atree:
			Atree[n] = self.unpackvalue(n,*Atree[n])
		return Atree
	def pool(self, key, name):
		for h,s,m,n,t,v in self.db.pool(key, name):
			yield h,s,m,n,self.unpackvalue(n,t,v)
	

def sqlite(dburl, architecture):
	"""
	Just redirector for possible different versions of stkdb formats
	"""
	logger = logging.getLogger("simtoolkit.database.sqlite")
	try:
		db = sqlite3.connect(dburl)
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot open data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot open data base file {} : {}".format(dburl, e))
	### Init DB IF NEEDED###
	try:
		db.execute("CREATE TABLE IF NOT EXISTS stk_attributes (attribute TEXT PRIMARY KEY , value TEXT);")
		db.commit()
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot create attributes table in data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot create attributes table in data base file {} : {}".format(dburl, e))
	for atr,value in ('version','0.1'),('architecture',architecture),('format','File'):
		try:
			db.execute("INSERT OR IGNORE INTO stk_attributes (attribute, value) VALUES(?,?);",(atr,value))
			db.commit()
		except BaseException as e:
			logger.error("----------------------------------------------------")
			logger.error("SimToolKit: DATABASE ERROR(sqlite)")
			logger.error("          : Cannot insert attribute {} into data base file {} : {}".format(atr,dburl, e))
			logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot insert attribute {} into data base file {} : {}".format(atr,dburl, e))
	try:
		db.execute("REPLACE INTO stk_attributes (attribute, value) VALUES('filename',?);",(dburl,))
		db.commit()
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot insert filename into data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot insert filename into data base file {} : {}".format(dburl, e))
	try:
		v = db.execute("SELECT value FROM stk_attributes WHERE attribute= \'version\' ;").fetchone()
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot fetch version attribute from data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot fetch version attribute from data base file {} : {}".format(dburl, e))
	if len(v) > 1:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : More than one version value ({}) in database {} ".format(v,dburl))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("More than one version value ({}) in database {} ".format(v,dburl))
	v = v[0]
	if v == "0.1":
		return sqlite_v_0_1(dburl, architecture)
	logger.error("----------------------------------------------------")
	logger.error("SimToolKit: DATABASE ERROR(sqlite)")
	logger.error("          : Unknown format version {} in data base file {} ".format(v,dburl))
	logger.error("----------------------------------------------------")		
	raise ValueError("Unknown format version {} in data base file {} ".format(v,dburl))

class sqlite_v_0_1:
	def __init__(self, dburl, architecture):
		self.logger = logging.getLogger("simtoolkit.database.sqlite_v_0_1")
		try:
			self.db = sqlite3.connect(dburl)
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__init__)")
			self.logger.error("          : Cannot open data base file {} : {}".format(dburl, e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot open data base file {} : {}".format(dburl, e))
		init_db =[
			'''CREATE TABLE IF NOT EXISTS stkrecords(
						   id        INTEGER PRIMARY KEY AUTOINCREMENT,
						   timestamp DATETIME,
						   hash      TEXT,
						   message   TEXT );
			''',
			'''CREATE TABLE IF NOT EXISTS stknames(
						   id        INTEGER PRIMARY KEY AUTOINCREMENT,
						   name      TEXT );
			''',
			'''CREATE TABLE IF NOT EXISTS stkvalues(
						   id        INTEGER PRIMARY KEY AUTOINCREMENT,
						   record    INTEGER,
						   name      INTEGER,
						   type      TEXT   DEFAULT 'TEXT',
						   value     BLOB );
			''',
			'''CREATE UNIQUE INDEX IF NOT EXISTS stkvalidx ON stkvalues (record,name);
			''',
			'''CREATE TABLE IF NOT EXISTS stktags(
						   id        INTEGER PRIMARY KEY AUTOINCREMENT,
						   record    INTEGER,
						   tag       TEXT );
			''',
			'''CREATE TEMPORARY VIEW IF NOT EXISTS stkview AS SELECT
					stkrecords.id        AS id,
					stkrecords.timestamp AS timestamp,
					stkrecords.hash      AS hash,
					stkrecords.message   AS message,
					stknames.name        AS name,
					stkvalues.name       AS nameid,
					stkvalues.type       AS type,
					stkvalues.value      AS value
					FROM stkvalues INNER JOIN stkrecords, stknames
					ON stkrecords.id=stkvalues.record AND stknames.id=stkvalues.name;
			''',
			'''CREATE TEMPORARY VIEW IF NOT EXISTS stktagview AS SELECT
					stkrecords.id        AS id,
					stkrecords.timestamp AS timestamp,
					stkrecords.hash      AS hash,
					stkrecords.message   AS message,
					stktags.tag          AS tag,
					stktags.id           AS tagid
					FROM stktags INNER JOIN stkrecords
					ON stkrecords.id=stktags.record ;
			''']
		for cmd in init_db:
			try:
				self.db.execute(cmd)
				self.db.commit()
			except BaseException as e :
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__init__)")
				self.logger.error("          : Cannot execute initiation sequence  {} : {}".format(cmd, e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot execute initiation sequence  {} : {}".format(cmd, e))
	def info(self):
		info = {}
		for atribute,value in self.db.execute("SELECT * FROM stk_attributes;"): info[atribute] = value
		info["py-sqlite"] = sqlite3.version
		info["sqlite"]    = sqlite3.sqlite_version
		for i,n,v in self.db.execute("PRAGMA database_list"):
			info["files/"+n]=v
		return info
		
		
	def mkrec(self,timestamp, rechash, message):
		try:
			cur = self.db.execute("INSERT INTO stkrecords (timestamp, hash, message) VALUES(:timestamp,:hash,:message);",
				{'timestamp':timestamp, 'hash':rechash, 'message':message})
			self.db.commit()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkrec)")
			self.logger.error("          : Cannot add a recored : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot add a recored : {}".format(e))
		try:
			recid = self.db.execute("SELECT id FROM stkrecords WHERE timestamp=:tiemstamp AND hash=:hash AND message=:message ;",
				{'tiemstamp':timestamp, 'hash':rechash, 'message':message}).fetchone()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkrec)")
			self.logger.error("          : Cannot fetch recored id : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch recored id : {}".format(e))
		if len(recid) > 1:
			self.logger.warning("----------------------------------------------------")
			self.logger.warning("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkrec)")
			self.logger.warning("          : There are more than one records with the same time stamp, hash and message")
			self.logger.warning("----------------------------------------------------")		
			#raise RuntimeError("There are more than one records with the same time stamp, hash and message")
		return cur.lastrowid
	def mkname(self,name):
		if "*" in name or "?" in name or "[" in name or "]" in name: 
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkname)")
			self.logger.error("          : name cannot contain *,?,]or[ charters: {} is given".format(name))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("name {} is not quintic".format(name))
		try:
			nameid = self.db.execute("SELECT id FROM stknames WHERE name=:name;",{'name':name}).fetchone()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkname)")
			self.logger.error("          : Cannot fetch name id : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch name id : {}".format(e))
		if not nameid is None:
			if len(nameid) > 1:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkname)")
				self.logger.error("          : name {} is not quintic".format(name))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("name {} is not quintic".format(name))
			else:
				return nameid[0]
		try:
			self.db.execute("INSERT OR IGNORE INTO stknames(name) VALUES(:name);",{'name':name})
			self.db.commit()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkname)")
			self.logger.error("          : Cannot add a name {} : {}".format(name,e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot add a name {} : {}".format(name,e))
		try:
			nameid = self.db.execute("SELECT id FROM stknames WHERE name=:name;",{'name':name}).fetchone()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkname)")
			self.logger.error("          : Cannot fetch name id : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch name id : {}".format(e))
		if len(nameid) > 1:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.mkname)")
			self.logger.error("          : name {} is not quintic".format(name))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("name {} is not quintic".format(name))
		return nameid[0]
	def recordvalue(self, n, recid, nameid, valtype, value):
		try:
			v = self.db.execute("SELECT id FROM stkvalues WHERE record=:record AND name=:name;",{'name':nameid, 'record':recid}).fetchone()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
			self.logger.error("          : Cannot fetch value id : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch value id : {}".format(e))
		if not v is None:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
			self.logger.error("          : There is another parameter with the same name {} in record {} ".format(n, recid))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("There is another parameter with the same name {} in record {} ".format(n, recid))
		try:
			self.db.execute("INSERT INTO stkvalues(record,name,type,value) VALUES(?,?,?,?);",
				[recid,nameid,valtype,value])
			
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
			self.logger.error("          : Cannot insert parameter {} in to record  {} : {}".format(n,recid,e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot insert parameter {} in to record  {} : {}".format(n,recid,e))
		
	def record(self, tree, timestamp, rechash, message):
		recid = self.mkrec(timestamp, rechash, message)
		for n in tree:
			nameid = self.mkname(n)
			try:
				self.recordvalue(n,recid,nameid,*tree[n])
			except BaseException as e :
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
				self.logger.error("          : Cannot record value {} in to record  {} : {}".format(n,recid,e))
				self.logger.error("          : Tree                                    : {}".format(tree))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot record value {}  in to record  {} : {}".format(n,recid,e))
				
		self.db.commit()
		return recid
#-------- NEED TO THINK ABOUT IT -----------------#
	def __setitem__(self, key, value):
		if isinstance(value,tree):
			if type(key) is tuple or type(key) is list :
				if len(key) == 2:
					if type(key[1]) is str or type(key[1]) is unicode:
						if key[1][-1] != "/": key[1] = key[1]+"/"
						for n in value:
							self[key[0],key[1]+n] = value[n]
					else:
						self.logger.error("----------------------------------------------------")
						self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__setitem__)")
						self.logger.error("          : To set the parameters tree by notaton db[record,parmaeter],  parameter must be a string {} is given".format(type(key[1])))
						self.logger.error("----------------------------------------------------")		
						raise TypeError("To set the parameters tree by notaton db[record,parmaeter],  parameter must be a string {} is given".format(type(key[1])))
				else:
					self.logger.error("----------------------------------------------------")
					self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__setitem__)")
					self.logger.error("          : Incorrect notation for seting a parameters tree. Should be db[record,parameter_name], db[{}] is given".format(key))
					self.logger.error("----------------------------------------------------")		
					raise TypeError("Incorrect notation for seting a parameters tree. Should be db[record,parameter_name], db[{}] is given".format(key))
			elif type(key) is str or type(key) is unicode or type(key) is int:
				for n in value:
					self[key,n] = value[n]
			else:
					self.logger.error("----------------------------------------------------")
					self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__setitem__)")
					self.logger.error("          : Incorrect type of key. It shoudl be string, unicode or int: {} is given".format(type(key)))
					self.logger.error("----------------------------------------------------")		
					raise TypeError("Incorrect type of key. It shoudl be string, unicode or int: {} is given".format(type(key)))
		elif (type(value) is tuple or type(value) is list ) and len(value) == 2:
			namescliner = []				
			if (type(key) is tuple or type(key) is list ) and len(key) == 2:
				rec,name = key
				if type(rec) is int:
					reci = [ self.db.execute("SELECT id FROM stkrecords WHERE id=:rec;",{'rec':rec}).fetchone()[0] ]
				elif type(rec) is str or type(rec) is unicode:
					reci = [ i for i, in self.db.execute("SELECT id FROM stkrecords WHERE hash GLOB :rec OR timestamp GLOB :rec;",{'rec':rec}) ]
				if   type(name) is int:
					nami = [ self.db.execute("SELECT * FROM stknames WHERE id=:name;",{'name':name}).fetchone()[0] ]
				elif type(name) is str or type(name) is unicode:
					for i,n in self.db.execute("SELECT * FROM stknames WHERE name GLOB :name;",{'name':name+"/*"}):
						namescliner.append(i)
					nrec = self.db.execute("SELECT * FROM stknames WHERE name GLOB :name;",{'name':name}).fetchall()
					if nrec is None or len(nrec) == 0:
						nami = [self.mkname(name)]#yield self.mkname(name)
					else           :
						nami = [ i for i,n in nrec ]
				
				vfl = [ {'rec':r,'name':n,'type':value[0],'value':value[1]} for r in reci for n in nami ]
				if len(vfl) == 0 :
					self.logger.error("----------------------------------------------------")
					self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__setitem__)")
					self.logger.error("          : Couldn't find record or name reci={}, namei={}".format(reci,nami))
					self.logger.error("----------------------------------------------------")		
					raise RuntimeError("Couldn't find record or name reci={}, namei={}".format(reci,nami))
				#DB>>
				print vfl 
																													
				self.db.executemany("REPLACE INTO stkvalues (record,name,type, value) VALUES (:rec,:name,:type,:value)",tuple(vfl))
				self.db.commit()
				#<<DB
				for i in namescliner:
					self.db.execute("DELETE FROM stknames WHERE id=?;",(i,))
				self.db.commit()
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__setitem__)")
				self.logger.error("          : key should be tuple and should have 2 entries, no more no less, {}:{} is given".format(key,len(key)))
				self.logger.error("----------------------------------------------------")		
				raise TypeError("key should have 2 entries, {} is given".format(len(key)))
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__setitem__)")
			self.logger.error("          : value should be a tupele and should have 2 entries, no more no less, {}:{} is given".format(value,len(value)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("key should have 2 entries, {} is given".format(len(key)))
	def __delitem__(self,key): pass #!!!!!
#-------- NEED TO THINK ABOUT IT -----------------#
	def __getitem__(self, key):
		if type(key) is int:
			SQL = "SELECT name,type, value FROM stkview WHERE id=:key;"
			name = None
		elif type(key) is str or type(key) is unicode:
			if key[-1]  == "/" : key  = key+"*"
			if "*" in key or "?" in key or "[" in key or "]" in key:
				SQL = "SELECT name,type, value FROM stkview WHERE timestamp GLOB :key OR hash GLOB :key;"
			else:
				SQL = "SELECT name,type, value FROM stkview WHERE timestamp=:key OR hash=:key;"
			name = None
		elif ( type(key) is tuple or type(key) is list ) and len(key) == 2:
			key,name = key
			SQL = "SELECT name,type, value FROM stkview WHERE "
			if type(key) is int: SQL += " id=:key"
			elif type(key) is str or type(key) is unicode:
				if "*" in key or "?" in key or "[" in key or "]" in key: SQL += " timestamp GLOB :key OR hash GLOB :key"
				else                                                   : SQL += " timestamp=:key OR hash=:key"
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__getitem__)")
				self.logger.error("          : Incorrect key type in tuple notation. It should be int or string. {}:{} is given".format(key,type(key)))
				self.logger.error("----------------------------------------------------")		
				raise TypeError("Incorrect key type in tuple notation. It should be int or string. {}:{} is given".format(key,type(key)))
			if type(name) is int: SQL += " AND nameid=:name"
			elif type(name) is str or type(name) is unicode:
				if name[-1]  == "/" : name  = name+"*"
				if "*" in name or "?" in name or "[" in name or "]" in name: 
					SQL += " AND name GLOB :name"
				else                                                       : SQL += " AND name=:name"
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__getitem__)")
				self.logger.error("          : Incorrect name type. It should be int or string. {}:{} is given".format(name,type(name)))
				self.logger.error("----------------------------------------------------")		
				raise TypeError("Incorrect name type. It should be int or string. {}:{} is given".format(name,type(name)))
			SQL += " ;"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__getitem__)")
			self.logger.error("          : Incorrect key type. It should be int or string or tuple of two. {}:{} is given".format(key,type(key)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect key type. It should be int or string or tuple of two. {}:{} is given".format(key,type(key)))
		try:
			Atree = tree()
			for name,tpy,val in self.db.execute(SQL,{'key':key,'name':name}):
				Atree[name]= tpy,val
			return Atree
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__getitem__)")
			self.logger.error("          : Cannot fetch items for key: {} : {}".format(key, e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch items for key: {} : {}".format(key, e))
	def __iter__(self):
		for rechash,timestemp,message in self.db.execute("SELECT hash,timestamp,message FROM stkrecords;"):
			yield rechash,timestemp,message
	def recs(self,flt=None,column=None):
		SQL = "SELECT id,hash,timestamp,message FROM stkrecords"
		if flt is None:			SQL += ";"
		elif type(flt) is int:	SQL += " WHERE id=:flt;"
		elif type(flt) is str or type(flt) is unicode:
			if column is None:
				SQL += " WHERE timestamp GLOB :flt OR hash GLOB :flt OR message GLOB :flt;"
			elif column == "timestamp":
				SQL += " WHERE timestamp GLOB :flt;"
			elif column == "hash":
				SQL += " WHERE  hash GLOB :flt ;"
			elif column == "message":
				SQL += " WHERE  message GLOB :flt;"
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.recs)")
				self.logger.error("          : Incorrect column for filter. It should be timestamp or hash or message: {} is given".format(column))
				self.logger.error("----------------------------------------------------")		
				raise ValueError("Incorrect column for filter. It should be timestamp or hash or message: {} is given".format(column))
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.recs)")
			self.logger.error("          : Incorrect filter type. It should be string. {} is given".format(type(flt)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect filter type. It should be string. {} is given".format(type(flt)))
		for recid,rechash,timestemp,message in self.db.execute(SQL,{'flt':flt}): yield recid,rechash,timestemp,message
	def names(self,flt=None):
		if flt is None:
			SQL = "SELECT id,name FROM stknames;"
		elif type(flt) is str or type(flt) is unicode:
			SQL = "SELECT id,name FROM stknames WHERE name GLOB :flt;"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.nanes)")
			self.logger.error("          : Incorrect filter type. It should be a string: {} is given".format(type(flt)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect filter type. It should be a string: {} is given".format(type(flt)))
		for nameid,name in self.db.execute(SQL,{'flt':flt}): yield nameid,name
	def values(self,flt=None,column=None):
		if flt is None:
			SQL = "SELECT id,record,name,type,value FROM stkvalues;"
		elif type(flt) is int:
			if column is None:
				SQL = "SELECT id,record,name,type,value FROM stkvalues WHERE record = :flt OR name = :flt;"
			elif column == "record":
				SQL = "SELECT id,record,name,type,value FROM stkvalues WHERE record = :flt;"
			elif column == "name":
				SQL = "SELECT id,record,name,type,value FROM stkvalues WHERE  name = :flt;"
			else:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.values)")
				self.logger.error("          : Incorrect column for filter. It should be record or name : {} is given".format(column))
				self.logger.error("----------------------------------------------------")		
				raise ValueError("Incorrect column for filter. It should be record or name : {} is given".format(column))
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.values)")
			self.logger.error("          : Incorrect filter type. It should be int: {} is given".format(type(flt)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect filter type. It should be int: {} is given".format(type(flt)))
		for valid,record,name,valtype,value in self.db.execute(SQL,{'flt':flt}): yield valid,record,name,valtype,value
	def pool(self, key, name):
		if name[-1] == "/" :name += "*"
		if type(key) is int:
			SQL = "SELECT hash,timestamp,message,name,type,value FROM stkview WHERE id=:key AND name GLOB :name;"
		elif type(key) is str or type(key) is unicode:
			SQL = "SELECT hash,timestamp,message,name,type,value FROM stkview WHERE (timestamp GLOB :key OR hash GLOB :key) AND name GLOB :name;"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.pool)")
			self.logger.error("          : Incorrect key type. It should be int or string. {} is given".format(type(key)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect key type. It should be int or string. {} is given".format(type(key)))
		try:
			for rechash,timestamp,message,name,valtype,value in self.db.execute(SQL,{'key':key,'name':name}):
				yield rechash,timestamp,message,name,valtype,value
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.pool)")
			self.logger.error("          : Cannot fetch value for key: {} and name {}: {}".format(key,name, e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch value for key: {} and name {}: {}".format(key,name, e))
	def poolrecs(self,key):
		if type(key) is int:
			SQL = "SELECT hash,timestamp,message FROM stkrecords WHERE id=:key ;"
		elif type(key) is str or type(key) is unicode:
			SQL = "SELECT hash,timestamp,message FROM stkrecords WHERE timestamp GLOB :key OR hash GLOB :key;"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.poolrecs)")
			self.logger.error("          : Incorrect key type. It should be int or string. {} is given".format(type(key)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect key type. It should be int or string. {} is given".format(type(key)))
		try:
			for rechash,timestamp,message in self.db.execute(SQL,{'key':key}):
				yield rechash,timestamp,message
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.poolrecs)")
			self.logger.error("          : Cannot fetch value for key: {} : {}".format(key, e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch value for key: {} : {}".format(key, e))
			
	def poolnames(self,key=None):
		if key is None:
			SQL = "SELECT name FROM stknames;"
		elif type(key) is int:
			SQL = "SELECT name FROM stknames WHERE id = :key;"
		elif type(key) is str or type(key) is unicode:
			
			SQL = "SELECT name FROM stknames WHERE name GLOB :key;"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.poolnames)")
			self.logger.error("          : Incorrect key type. It should be int or string. {} is given".format(type(key)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect key type. It should be int or string. {} is given".format(type(key)))	
		try:
			for name in self.db.execute(SQL,{'key':key}):yield name
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.poolnames)")
			self.logger.error("          : Cannot fetch names for any key {} : {}".format(key,e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch names for any key {} : {}".format(key,e))
	def settag(self,key,tag):
		if "%" in tag or "*" in tag:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.settag)")
				self.logger.error("          : Tag cannot contains * or % characters. Given {}".format(tag))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Tag cannot contains * or % characters. Given {}".format(tag))
		if type(key) is int:
			SQL = "SELECT id FROM stkrecords WHERE id=:key;"
		elif type(key) is str or type(key) is unicode:
			if "%" in key:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.settag)")
				self.logger.error("          : key cannot contains % character. Given {}".format(key))
				self.logger.error("----------------------------------------------------")		
				raise ValueError("key cannot contains % character. Given {}".format(key))
			SQL = "SELECT id FROM stkrecords WHERE timestamp=:key OR hash=:key;"
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.settag)")
			self.logger.error("          : Incorrect key type. It should be int or string. {} is given".format(type(key)))
			self.logger.error("----------------------------------------------------")		
			raise TypeError("Incorrect key type. It should be int or string. {} is given".format(type(key)))
		for recid, in self.db.execute(SQL,{'key':key}):
			try:
				self.db.execute("INSERT INTO stktags(record,tag) VALUES(?,?);",[recid,str(tag)])
				self.db.commit()
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.settag)")
				self.logger.error("          : Cannot set tag: {} for key {}: {}".format(tag,key,e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot set tag: {} for key ;{}: {}".format(tag,key,e))
	def rmtag(self,key):
		try:
			self.db.execute("DELETE FROM stktags WHERE tag GLOB :key;",{'key':key})
			self.db.commit()
		except BaseException as e:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.rmtag)")
			self.logger.error("          : Cannot remove tag {} : {}".format(tag,e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot remove tag {} : {}".format(tag,e))
	def pooltags(self, key=None):
		if key is None:
			SQL = "SELECT tag,id,timestamp,hash,message FROM stktagview ;"
		if type(key) is int:
			SQL = "SELECT tag,id,timestamp,hash,message FROM stktagview WHERE tagid=:key;"
		elif type(key) is str or type(key) is unicode:
			SQL = "SELECT tag,id,timestamp,hash,message FROM stktagview WHERE tag GLOB :key;"
		for tag,recid,timestamp,rechash,message in self.db.execute(SQL,{'key':key}):
			yield tag,recid,timestamp,rechash,message
	def tags(self):
		for tag,recid in self.db.execute("SELECT tag,id FROM stktagview"):
			yield tag,recid

			
		
if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "USEAGE: python -m simtoolkit/database model-fileformats/example.stkdb"
		exit(1)
	testdb = db(sys.argv[1])
	for row in testdb.db.db.execute("SELECT * FROM stk_attributes;"):
		print row[0],"=",row[1]
	#DB>>
	#for row in testdb.db.db.execute("SELECT * FROM stkview;"):
		#print row
	#for row in testdb.db.db.execute("SELECT * FROM stkvalues;"):
		#print row
	#<<DB
	#DB>>
	x=tree()
	x["/a/b/s"]='2'
	x["/a/b/k"]='5'
	x["/list" ]=[1,2,3,4,5]
	x["/array"]=np.random.rand(10)
	recid = testdb.record(x,"Blash-blash-blash")
	for h,t,m in testdb:
		print h,t,m
		At = testdb[h]
		for p,k in At.printnames():
			if k is None:
				print p
			else:
				print p,At[k]
		print 

	print "\n/list  :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","/list"):
		print l
	print "\n/li    :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","/li"):
		print l
	print "\n/li*   :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","/li*"):
		print l
	print "\n/a/   :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","/a/"):
		print l
	print "\n/a    :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","/a"):
		print l
	print "\n/a*   :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","/a*"):
		print l
	print "\n/     :"
	for l in testdb.pool("ab2d5b661a598a58b5f96b967c42be524fc56318","*"):
		print l
	print "\nkey* /* :"
	for l in testdb.pool("d*","/*"):
		print l
	print
	#print "=== TAGS ==="
	#print " > SET TAG"
	#tag = "%04d"%(np.random.randint(9999))
	#testdb.settag(recid,tag)
	#for tag,recid,timestamp,rechash,message in testdb.pooltags():
		#print "TAG        = ", tag
		#print "RECID      = ", recid
		#print "TIME STAMP = ", timestamp
		#print "HASH       = ", rechash
		#print "MESSAGE    = ", message
		#print "=============="
		#print
	#for tag,recid in testdb.tags():
		#print tag,recid
	
