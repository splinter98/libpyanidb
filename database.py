import MySQLdb

def parse_database_url(url):
	proto,url=url.split('://',1)
	host,url=url.split('/',1)
	db,url=url.split('?',1)
	pars=url.split('&')
	for par in pars:
		key,value=par.split('=',1)
		if key=='user':
			user=value
		elif key=='password':
			password=value
	return proto,host,db,user,password

class Database(object):
	def __init__(self,server,database,username,password):
		self.server,self.database,self.username,self.password=server,database,username,password
		self.connection=MySQLdb.connect(host=server,user=username,passwd=password,db=database)
		self.db=self.connection.cursor()
		self.log=None
	
	def _query(self,*args):
		args=list(args)
		for i in range(args.count('')):
			args.remove('')
		return ' '.join(args)
	
	def execute(self,query,*args):
		if self.log:
			self.log("DbIO > "+query%self.connection.literal(args))
		if args==():
			self.db.execute(query)
		else:
			self.db.execute(query,args)
		result=list(self.db.fetchall())
		if self.log:
			self.log("DbIO < "+str(result))
		return result
	
	def new(self,table,variable):
		value=self.select(table,"MAX(%s)"%(variable),'')[0][0]
		if not value:
			value=0
		return value+1
	
	def select(self,table,variables,condition,*args):
		qry=self._query("SELECT "+variables,
				"FROM "+table,
				condition and "WHERE "+condition or '')
		return self.execute(qry,*args)

	def insert(self,table,variables,values,*args):
		qry=self._query("INSERT INTO "+table,
				variables and "(%s)"%variables or '',
				"VALUES ("+values+")")
		return self.execute(qry,*args)

	def update(self,table,variables,condition,*args):
		qry=self._query("UPDATE "+table,
				"SET "+variables,
				condition and "WHERE "+condition or '')
		return self.execute(qry,*args)

	def delete(self,table,condition,*args):
		qry=self._query("DELETE FROM "+table,
				condition and "WHERE "+condition or '')
		return self.execute(qry,*args)
	
