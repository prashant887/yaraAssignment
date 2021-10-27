from db_conn import Connect


def load_table(csv_file_name,table_name,cursor):
    #sql = "COPY yara.{} FROM STDIN DELIMITER ',' CSV HEADER".format(table_name)
    #print(" SQL: ",sql)
    #cursor.copy_expert(sql, open(csv_file_name, "r"))

    sql_str = '''\copy yara.{} FROM '{}' DELIMITER ',' CSV HEADER;'''.format(table_name,csv_file_name)
    print(sql_str)
    cursor.execute(sql_str)




try:
    conn=Connect(user='postgres',password='postgres',dbname='postgres',host='localhost')
    print(conn.conn)
    cur=conn.get_cur()


    cur.execute("select * from yara.user_location_stage")
    print(cur.fetchone())

    table_loads=[
        { "table":"user_location_stage","path":"/Users/pl465j/projects/yara/user_location.csv"}
        ]
    for table in table_loads:
        load_table(csv_file_name=table.get('path'),table_name=table.get('table'),cursor=cur)
    
except Exception as e:
    print(str(e))
finally:
    conn.close_cur()
    conn.close_db()