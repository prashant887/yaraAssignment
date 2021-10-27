import pandas as pd
from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres')


df = pd.read_csv('/Users/pl465j/projects/yara/user_location.csv')

#print(df['latitude'],df['latitude'].str.contains('^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$', regex=True))

#newDF=df[df['user_id']==29201]

#print(newDF)

#print(newDF['longitude'],newDF['longitude'].str.contains('^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$', regex=True))

filteredDF=df[df['longitude'].str.contains('^[+-]?([0-9]+\.?[0-9]*|\.[0-9]+)$', regex=True)]
print(filteredDF)
##df.to_sql('user_location_stage', engine, if_exists='replace', index=False,schema='yara')



#newdf = pd.read_sql_query('select * from user_location_stage',con=engine)
#print(newdf)