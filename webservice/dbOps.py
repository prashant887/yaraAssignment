from flask import json
import psycopg2
import pandas as pd 

class dbOps:
    def __init__(self) -> None:
        try:
            self.conn  = psycopg2.connect(host='localhost',database='postgres',user='postgres',password='postgres')
        except Exception as e:
            print(str(e))

    def close_conn(self):
        if self.conn is not None:
            self.conn.close()

    def getHistTab(self):
        sql="""with tab_one (location_id,address,user_id,city,distance) as (
select location_id,address,user_id,city,point(longitude::float8,latitude::float8) <@> point(lng::float,lat::float) distance
FROM yara.user_location, yara.world_cities) ,
tab_two(location_id,address,user_id,city,distance,rnk)  
as (select location_id,address,user_id,city,distance,row_number() over(partition by user_id order by distance) from tab_one),
 tab_three(location_id,address,user_id,city,distance) as (
select location_id,address,user_id,city,distance from tab_two where rnk=1 ),tab_four(hist,freq) as (
select '0-5',count(*) from tab_three where distance > 0 and distance<=5
union
select '5-10',count(*) from tab_three where distance > 5 and distance<=10
union
select '10-15',count(*) from tab_three where  distance > 10 and distance<=15
union
select '15-20',count(*) from tab_three where distance > 15 and distance<=20
union
select '20-25',count(*) from tab_three where distance > 20 and distance<=25
union
select '25-30',count(*) from tab_three where distance > 25 and distance<=30
union
select '30-35',count(*) from tab_three where distance > 30 and distance<=35
union
select '>35',count(*) from tab_three where distance > 35 )
select * from tab_four order by hist
;
"""
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            rows = pd.DataFrame(cur.fetchall(), columns=['HIST','FREQ'])
            dfhtml = rows.to_html()
            return dfhtml
        except Exception as e:
            print(str(e))
        finally:
            if cur is not None:
                cur.close()
            self.close_conn()    

    def getHist(self):
        sql="""with tab_one (location_id,address,user_id,city,distance) as (
select location_id,address,user_id,city,point(longitude::float8,latitude::float8) <@> point(lng::float,lat::float) distance
FROM yara.user_location, yara.world_cities) ,
tab_two(location_id,address,user_id,city,distance,rnk)  	
as (select location_id,address,user_id,city,distance,row_number() over(partition by user_id order by distance) from tab_one),
 tab_three(location_id,address,user_id,city,distance) as (
select location_id,address,user_id,city,distance from tab_two where rnk=1 ),tab_four(hist,freq) as (
select '0-5',count(*) from tab_three where distance > 0 and distance<=5
union
select '5-10',count(*) from tab_three where distance > 5 and distance<=10
union
select '10-15',count(*) from tab_three where  distance > 10 and distance<=15
union
select '15-20',count(*) from tab_three where distance > 15 and distance<=20
union
select '20-25',count(*) from tab_three where distance > 20 and distance<=25
union
select '25-30',count(*) from tab_three where distance > 25 and distance<=30
union
select '30-35',count(*) from tab_three where distance > 30 and distance<=35
union
select '>35',count(*) from tab_three where distance > 35 )
select     json_agg(tab_four) from tab_four 
"""
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            jsn=cur.fetchone()[0]
            return {"code":200,"result":jsn}
        except Exception as e:
            print(str(e))
            return {"code":400,"result":str(e)}
        finally:
            if cur is not None:
                cur.close()
            self.close_conn() 
    
    def getLocDetails(self,req):
        if not isinstance(req,dict):
            return {"code":400,"result":"Request Body Should be a Json"}
        
        if 'location_id' not in req:
            return {"code":400,"result":"Request Body expects location_id Field"}

        loc_id=req.get('location_id',0)
        sql="""select row_to_json(t.*) from yara.user_location t where t.location_id={}""".format(loc_id)
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            jsn=cur.fetchone()
            if jsn is None:
                return  {"code":400,"result":"No Data Found for Loc Id {}".format(loc_id)}
            return  {"code":200,"result":jsn}
        except Exception as e:
            print(str(e))
            return {"code":400,"result":str(e)}
        finally:
            if cur is not None:
                cur.close()
            self.close_conn() 
    
    def getLocDetailsTab(self,loc_id):
        sql="""select row_to_json(t.*) from yara.user_location t where t.location_id={}""".format(loc_id)
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            rows = pd.DataFrame(cur.fetchall(), columns=['ADDRESS', 'LATITUDE', 'LONGITUDE', 'TAG', 'LOCATION_ID', 'USER_ID'])
            dfhtml = rows.to_html()
            return dfhtml
        except Exception as e:
            print(str(e))
        finally:
            if cur is not None:
                cur.close()
            self.close_conn() 

    def addLocDetails(self,req):
        if not isinstance(req,dict):
            return {"code":400,"result":"Request Body Should be a Json"}
        
        if 'address' not in req:
            return {"code":400,"result":"Request Body expects address Field"}

        if 'latitude' not in req:
            return {"code":400,"result":"Request Body expects latitude Field"}

        if 'location_id' not in req:
            return {"code":400,"result":"Request Body expects location_id Field"}

        if 'longitude' not in req:
            return {"code":400,"result":"Request Body expects longitude Field"}
        
        if 'tag' not in req:
            return {"code":400,"result":"Request Body expects tag Field"}
        
        if 'user_id' not in req:
            return {"code":400,"result":"Request Body expects user_id Field"}
        
        address=req.get('address','')
        latitude=req.get('latitude','')
        location_id=req.get('location_id','')
        longitude=req.get('longitude','')
        tag=req.get('tag','')
        user_id=req.get('user_id','')
        sql="""insert into yara.user_location(address,latitude,longitude,tag,location_id,user_id)
        values('{}',{},{},'{}',{},{})""".format(address,latitude,longitude,tag,location_id,user_id)
        print(sql)
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            sql="""select row_to_json(t.*) from yara.user_location t where t.location_id={}""".format(location_id)
            cur.execute(sql)
            jsn=cur.fetchone()[0]
            return {"code":200,"result":jsn}
        except Exception as e:
            print(str(e))
            return {"code":400,"result":str(e)}
        finally:
            if cur is not None:
                cur.close()
            self.close_conn() 
    
    def updLocDetails(self,req):
        sql=None
        if not isinstance(req,dict):
            return {"code":400,"result":"Request Body Should be a Json"}

        if 'location_id' not in req:
            return {"code":400,"result":"Request Body expects location_id Field"}

        location_id=req.get('location_id','')
        
        if 'address'  in req:
            address=req.get('address','')
            if sql is None:
                sql="address = '{}'".format(address)
            else:
                sql=sql+",address = '{}'".format(address)

        if 'latitude'  in req:
            latitude=req.get('latitude','')
            if sql is None:
                sql="latitude = {}".format(latitude)
            else:
                sql=sql+",latitude = {}".format(latitude)

        

        if 'longitude'  in req:
            longitude=req.get('longitude','')
            if sql is None:
                sql="longitude = {}".format(longitude)
            else:
                sql=sql+",longitude = {}".format(longitude)
        
        if 'tag'  in req:
            tag=req.get('tag','')
            if sql is None:
                sql="tag = '{}'".format(tag)
            else:
                sql=sql+",tag = '{}'".format(tag)
        
        if 'user_id'  in req:
            user_id=req.get('user_id','')
            if sql is None:
                sql="user_id = {}".format(user_id)
            else:
                sql=sql+",user_id = {}".format(user_id)

        if sql is not None:
            upd_stmt="""update yara.user_location  set {} where location_id={}""".format(sql,location_id)
            print(upd_stmt)
            try:
                cur=self.conn.cursor()
                cur.execute(upd_stmt)
                self.conn.commit()
                sql="""select row_to_json(t.*) from yara.user_location t where t.location_id={}""".format(location_id)
                cur.execute(sql)
                jsn=cur.fetchone()[0]
                return {"code":200,"result":jsn}
            except Exception as e:
                 print(str(e))
                 return {"code":400,"result":str(e)}
            finally:
                if cur is not None:
                    cur.close()
                self.conn.close()       
        return  {"code":200,"result":"Nothing to Update"}
        
        
        
        

    def delLocDetails(self,req):
        if not isinstance(req,dict):
            return {"code":400,"result":"Request Body Should be a Json"}
        
        if 'location_id' not in req:
            return {"code":400,"result":"Request Body expects location_id Field"}

        
        location_id=req.get('location_id','')
        sql="""delete from yara.user_location where location_id={}""".format(location_id)
        print(sql)
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            return  {"code":200,"result":"Data Deleted Successfuly"}
        except Exception as e:
            print(str(e))
            return {"code":400,"result":str(e)}
        finally:
            if cur is not None:
                cur.close()
            self.close_conn() 
