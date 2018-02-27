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
		self.db.record(tree, timestamp, rechash, message)
	def __setitem__(self, key, value):
		self.logger.error("----------------------------------------------------")
		self.logger.error("SimToolKit: DATABASE ERROR(db.__setitem__)")
		self.logger.error("          : Notation db[key] = value is not valuable set for data base ")
		self.logger.error("          : Use function db.record() to record to a data base")
		self.logger.error("          : Use function db.update() to correct an existing record")
		self.logger.error("          : Notation db[hash] or db[timestamp] should be used only for reading data base")
		self.logger.error("----------------------------------------------------")		
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
		for n,t,v in self.db.pool(key, name):
			yield n,self.unpackvalue(n,t,v)
	def __iter__(self):
		for rechash,timestemp,message in self.db:
			yield rechash,timestemp,message

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
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot create attributes table in data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot create attributes table in data base file {} : {}".format(dburl, e))
	try:
		db.execute("INSERT OR IGNORE INTO stk_attributes (attribute, value) VALUES('version','0.1');")
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot insert version attribute into data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot insert version attribute into data base file {} : {}".format(dburl, e))
	try:
		db.execute("INSERT OR IGNORE INTO stk_attributes (attribute, value) VALUES('architecture',?);",[architecture])
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot insert architecture attribute into data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot insert architecture attribute into data base file {} : {}".format(dburl, e))
	try:
		db.execute("INSERT OR IGNORE INTO stk_attributes (attribute, value) VALUES('format','File');")
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot insert format attribute into data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot insert format attribute into data base file {} : {}".format(dburl, e))
	try:
		db.commit()
	except BaseException as e:
		logger.error("----------------------------------------------------")
		logger.error("SimToolKit: DATABASE ERROR(sqlite)")
		logger.error("          : Cannot commit data base file {} : {}".format(dburl, e))
		logger.error("----------------------------------------------------")		
		raise RuntimeError("Cannot commit data base file {} : {}".format(dburl, e))
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
			'''CREATE TEMPORARY VIEW IF NOT EXISTS stkview AS SELECT
					stkrecords.id        AS id,
					stkrecords.timestamp AS timestamp,
					stkrecords.hash      AS hash,
					stkrecords.message   AS message,
					stknames.name        AS name,
					stkvalues.type       AS type,
					stkvalues.value      AS value
					FROM stkvalues INNER JOIN stkrecords, stknames
					ON stkrecords.id=stkvalues.record AND stknames.id=stkvalues.name;
			''']
		for cmd in init_db:
			try:
				self.db.execute(cmd)
			except BaseException as e :
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__init__)")
				self.logger.error("          : Cannot execute initiation sequence  {} : {}".format(cmd, e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot execute initiation sequence  {} : {}".format(cmd, e))
		try:
			self.db.commit()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__init__)")
			self.logger.error("          : Cannot commit data base initialization : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot commit data base initialization : {}".format(e))
		
	def record(self, tree, timestamp, rechash, message):
		try:
			self.db.execute("INSERT OR IGNORE INTO stkrecords (timestamp, hash, message) VALUES(:timestamp,:hash,:message);",
				{'timestamp':timestamp, 'hash':rechash, 'message':message})
			self.db.commit()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
			self.logger.error("          : Cannot add a recored : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot add a recored : {}".format(e))
		try:
			recid = self.db.execute("SELECT id FROM stkrecords WHERE timestamp=:tiemstamp AND hash=:hash AND message=:message ;",
				{'tiemstamp':timestamp, 'hash':rechash, 'message':message}).fetchone()
		except BaseException as e :
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
			self.logger.error("          : Cannot fetch recored id : {}".format(e))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Cannot fetch recored id : {}".format(e))
		if len(recid) > 1:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
			self.logger.error("          : There are more than one records with the same time stamp, hash and message")
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("There are more than one records with the same time stamp, hash and message")
		recid = recid[0]
		for n in tree:
			try:
				self.db.execute("INSERT OR IGNORE INTO stknames(name) VALUES(:name);",{'name':n})
				self.db.commit()
			except BaseException as e :
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
				self.logger.error("          : Cannot add a name {} : {}".format(n,e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot add a name {} : {}".format(n,e))
			try:
				nameid = self.db.execute("SELECT id FROM stknames WHERE name=:name;",
					{'name':n}).fetchone()
			except BaseException as e :
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
				self.logger.error("          : Cannot fetch name id : {}".format(e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot fetch name id : {}".format(e))
			if len(nameid) > 1:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
				self.logger.error("          : name {} is not quintic".format(n))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("name {} is not quintic".format(n))
			nameid = nameid[0]
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
				self.logger.error("          : There is another parameter with the same name {} in record {}".format(n, recid))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("There is another parameter with the same name {} in record {}".format(n, recid))
			valtype,value = tree[n]
			try:
				self.db.execute("INSERT INTO stkvalues(record,name,type,value) VALUES(?,?,?,?);",
					[recid,nameid,valtype,value])
				self.db.commit()
			except BaseException as e :
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.record)")
				self.logger.error("          : Cannot insert parameter {} in to record  {} : {}".format(n,recid,e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot insert parameter {} in to record  {} : {}".format(n,recid,e))
	def __getitem__(self, key):
		Atree = tree()
		if type(key) is int:
			for name,tpy,val in self.db.execute("SELECT name,type, value FROM stkview WHERE id=:key;",{'key':key}):
				Atree[name]= tpy,val
			return Atree
		elif type(key) is str or type(key) is unicode:
			for name,tpy,val in self.db.execute("SELECT name,type, value FROM stkview WHERE timestamp=:key OR hash=:key;",{'key':key}):
				Atree[name]= tpy,val
			return Atree
		self.logger.error("----------------------------------------------------")
		self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.__getitem__)")
		self.logger.error("          : Incorrect key type. It should be int or string. {} is given".format(type(key)))
		self.logger.error("----------------------------------------------------")		
		raise RuntimeError("Incorrect key type. It should be int or string. {} is given".format(type(key)))
	def __iter__(self):
		for rechash,timestemp,message in self.db.execute("SELECT hash,timestamp,message FROM stkrecords;"):
			yield rechash,timestemp,message			
	def pool(self, key, name):
		if type(key) is int:
			try:
				for name,valtype,value in self.db.execute("SELECT name,type,value FROM stkview WHERE id=:key AND name LIKE :name;",{'key':key,'name':name+"%"}):
					yield name,valtype,value
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.pool)")
				self.logger.error("          : Cannot fetch value for key: {} and name ;{}: {}".format(key,name, e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot fetch value for key: {} and name ;{}: {}".format(key,name, e))
			
		elif type(key) is str or type(key) is unicode:
			try:
				for name,valtype,value in self.db.execute("SELECT name,type,value FROM stkview WHERE (timestamp=:key OR hash=:key) AND name LIKE :name;",{'key':key,'name':name+"%"}):
					yield name,valtype,value
			except BaseException as e:
				self.logger.error("----------------------------------------------------")
				self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.pool)")
				self.logger.error("          : Cannot fetch value for key: {} and name ;{}: {}".format(key,name, e))
				self.logger.error("----------------------------------------------------")		
				raise RuntimeError("Cannot fetch value for key: {} and name ;{}: {}".format(key,name, e))
		else:
			self.logger.error("----------------------------------------------------")
			self.logger.error("SimToolKit: DATABASE ERROR(sqlite_v_0_1.pool)")
			self.logger.error("          : Incorrect key type. It should be int or string. {} is given".format(type(key)))
			self.logger.error("----------------------------------------------------")		
			raise RuntimeError("Incorrect key type. It should be int or string. {} is given".format(type(key)))
			
		
if __name__ == "__main__":
	if len(sys.argv) < 2:
		print "USEAGE: python -m simtoolkit/database model-fileformats/example.stkdb"
		exit(1)
	testdb = db(sys.argv[1])
	for row in testdb.db.db.execute("SELECT * FROM stk_attributes;"):
		print row[0],"=",row[1]
	#DB>>
	x=tree()
	x["/a/b/s"]='2'
	x["/a/b/k"]='5'
	x["/list" ]=[1,2,3,4,5]
	x["/array"]=np.random.rand(10)
	testdb.record(x,"Blash-blash-blash")
	for h,t,m in testdb:
		print h,t,m
		At = testdb[h]
		for p,k in At.printnames():
			if k is None:
				print p
			else:
				print p,At[k]
		print 

	print "\n/list:"
	for l in testdb.pool("a0d52e9134cbd7bda20c567134dede2a7d71a57e","/list"):
		print l
	print "\n/a/:"
	for l in testdb.pool("a0d52e9134cbd7bda20c567134dede2a7d71a57e","/a/"):
		print l
	print "\n/a:"
	for l in testdb.pool("a0d52e9134cbd7bda20c567134dede2a7d71a57e","/a"):
		print l
