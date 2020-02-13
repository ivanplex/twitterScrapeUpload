import pymysql
import datetime as dt

class LogDB:

    def ifTableExist(self, tablename):
        stmt = "SHOW TABLES LIKE '{name}'".format(name=tablename)
        self.cursor.execute(stmt)
        result = self.cursor.fetchone()
        if result:
            print("Table exist")
            return True
        else:
            return False
    
    def createTable(self):
        stmt = """
            CREATE TABLE `twitter_log` (
            `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
            `start_date` date DEFAULT NULL,
            `end_date` date DEFAULT NULL,
            `crawl_start` timestamp NULL DEFAULT NULL,
            `crawl_end` timestamp NULL DEFAULT NULL,
            `context` text,
            `success` boolean,
            `length` int(11) NOT NULL DEFAULT 0,
            PRIMARY KEY (`id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
        """
        self.cursor.execute(stmt)
        self.connection.commit()

    def __init__(self, host, user, password, db, tablename):
        self.connection = pymysql.connect(
                            host=host,
                            user=user,
                            password=password,
                            db=db)
        self.cursor = self.connection.cursor()
        if not self.ifTableExist(tablename):
            self.createTable()

    def insertRow(self, sql, values):
        # Insert row to DB
        # sql: SQL prepared statement 
        # values: prepared values
        # return success, rowID
        try:
            self.cursor.execute(sql, values)
            self.connection.commit()
            return True, self.cursor.lastrowid
        except pymysql.err.InternalError:
            return False, None
    
    def updateRow(self, sql, values):
        # update row
        # sql: SQL prepared statement 
        # values: prepared values
        # return success, message
        try:
            self.cursor.execute(sql, values)
            self.connection.commit()
            return True, str(self.cursor.rowcount)+" record(s) affected"
        except pymysql.err.InternalError:
            return False, None

    def query(self, sql):
        # Fetch many rows
        try:
            self.cursor.execute(sql)
            return True, self.cursor.fetchall()
        except pymysql.err.InternalError:
            return False, None
        
    def queryOne(self, sql):
        # Fetch 1 row
        try:
            self.cursor.execute(sql)
            return True, self.cursor.fetchone()
        except pymysql.err.InternalError:
            return False, None

class TwitterLogDB(LogDB):

    def __init__(self, host, user, password, db, tablename):
        super().__init__(host, user, password, db, tablename)
    
    def startCrawl(self, start_date, end_date, context):
        # Insert row to DB
        # stmt: SQL statement
        # return success, message
        sql = "INSERT INTO twitter_log (start_date, end_date, crawl_start, context, success) VALUES (%s, %s, %s, %s, %s)"
        val = (start_date, end_date, dt.datetime.now(), context, False)
        return self.insertRow(sql, val)

    def endCrawl(self, id, length):
        sql = "UPDATE twitter_log SET success='1', crawl_end=%s, length=%s WHERE id=%s"
        val = (dt.datetime.now(), id, length)
        return self.updateRow(sql, val)

    def earliestQuery(self):
        # reverse Find the next day to crawl
        # return None if no record
        sql = "SELECT start_date FROM twitter_log ORDER BY start_date LIMIT 1"
        success, result = self.queryOne(sql)
        if result:
            return result[0]
        else:
            return None
    