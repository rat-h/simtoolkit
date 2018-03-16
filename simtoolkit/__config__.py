import os, imp, logging
from ConfigParser import ConfigParser
from os.path import expanduser

CONFFILENAME=".simtoolkitrc"

logger = logging.getLogger("simtoolkit.__config__")
def readconf(rc,dic=None):
	config = ConfigParser()
	config.optionxform=str
	try:
		config.read( rc )
	except BaseException as e:
		raise ValueError("Config Parser returns an error\'{}\'".format(e))
	if dic is None:
		dic={}
	elif not type(dic) is dict:
		raise TypeError("Incorrect dictionary type")
	for section in config.sections():
		if not section in dic:
			dic[section]={}
		elif not type(dic[section]) is dict:
			raise TypeError("Section {} in dictionary is not a dictionary")
		for option in config.options(section):
			dic[section][option] = config.get(section,option)
			
	return dic
		
		
_config = {
	'GENERAL':{
		'editor' : 'vi',
	},
}


loc = ["/etc","/usr/local/etc"]
try:
	_,stkloc,_ = imp.find_module('simtoolkit')
	loc.append(stkloc)
except BaseException as e: pass
try:
	_,cnfloc,_ = imp.find_module('__config__')
	loc.append(os.path.dirname(cnfloc))
except BaseException as e: pass

loc.append( expanduser("~") )
loc.append(".")

for l in loc:
	if os.access(l+"/"+CONFFILENAME, os.R_OK):
		logger.debug("Reading {}".format(l+"/"+CONFFILENAME))
		_config = readconf(l+"/"+CONFFILENAME, _config)

from tree import tree as tr
stk_config = tr()
for n in _config:
	stk_config[n] = _config[n]
