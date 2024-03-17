import csv
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, ForeignKey, Date
from sqlalchemy import create_engine
from datetime import datetime

 
engine = create_engine('sqlite:///database.db')
meta = MetaData()

## Create database
stations = Table(
      'clean_stations', meta,
      Column('id', String, primary_key=True),
      Column('latitude', Float),
      Column('longitude', Float),
      Column('elevatior', Float),
      Column('name', String),
      Column('country', String),
      Column('state', String)
   )

measures = Table(
      'station_measures', meta,
      Column('station_id', String, ForeignKey("clean_stations.id"), nullable=False),
      Column('date', Date),
      Column('precip', Float),
      Column('tobs', Integer)
   )


with engine.connect() as conn:
   meta.create_all(conn)

add_files = False
if add_files:
   #Load files into database
   with open('clean_stations.csv') as csvfile:
      reader = csv.reader(csvfile, delimiter=',')
      next(reader)

      with engine.connect() as conn:
         for line in reader:
            ins = stations.insert().values(id=line[0], latitude=line[1], longitude=line[2],
                                  elevatior=line[3], name=line[4], country=line[5],
                                  state=line[6])
            conn.execute(ins)
      

   with open('clean_measure.csv') as csvfile:
      reader = csv.reader(csvfile, delimiter=',')
      next(reader)

      with engine.connect() as conn:
         for line in reader:
            ins = measures.insert().values(station_id=line[0], 
                                       date=datetime.strptime(line[1], '%Y-%m-%d').date(),
                                       precip=line[2], tobs=line[3])
            conn.execute(ins)

with engine.connect() as conn:
   result = conn.execute("SELECT * FROM clean_stations LIMIT 5").fetchall()
   print(result)
