import psycopg2

class Connect():
    def __init__(self,user,password,host,dbname) -> None:
        self.user=user
        self.password=password
        self.host=host
        self.dbname=dbname

        try:
            self.conn  = psycopg2.connect(host=host,database=dbname,user=user,password=password)
            cur = self.conn.cursor()
        
	# execute a statement
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')

        # display the PostgreSQL database server version
            db_version = cur.fetchone()
            #print(db_version)
       

        except Exception as e:
            raise Exception(str(e))


    def get_cur(self):
        self.cur= self.conn.cursor()
        return self.cur

    def close_cur(self):
        if self.cur is not None:
            self.cur.close()


    def close_db(self):
        if self.conn is not None:
            self.conn.close()


if __name__=="__main__":
    conn=Connect(user='postgres',password='postgres',dbname='postgres',host='localhost')
    cur=conn.get_cur()
    cur.execute("select version()")
    print(cur.fetchone())
    conn.close_cur()
    conn.close_db()


    
