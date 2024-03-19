import csv
from sqlalchemy import Table, Column, Integer, String, MetaData, Float, ForeignKey, Date
from sqlalchemy import create_engine, select
from datetime import datetime

### Functions

def print_all_stations(conn):
   '''
   Display all of the clean stations from the database
   :param conn: Connection object
   :return: list of station ids
   '''
   query = select([stations.c.id])
   result = conn.execute(query).fetchall()
   return result

def select_station_between_dates(conn, station_id, *args):
   '''
   Query measures for selected station between given days
   :param conn: Connection object
   :param station_id: station id
   :prama *args: days of intrest in format (YYYY-MM-DD)
   :return: list of all measures 
   '''
   j = stations.join(measures, stations.c.id == measures.c.station_id)
   parameters = [datetime.strptime(k, '%Y-%m-%d').date() for k in args]

   query = measures.select().select_from(j).where((stations.c.id == station_id) &
                                                (measures.c.date >= min(parameters)) &
                                                (measures.c.date <= max(parameters)))
   result = conn.execute(query)
   return result

def update_measures_tobs(conn, station_id, date, new_value):
   '''
   Update tobs value in measure table
   :param conn: Connection object
   :param station_id: station id
   :param date: date in format (YYYY-MM-DD)
   :param new_value: value to be inserted
   '''

   update = measures.update().where((measures.c.station_id == station_id) &
                                    (measures.c.date == datetime.strptime(date, '%Y-%m-%d').date())).\
                                    values(tobs=new_value)
   
   conn.execute(update)


def delete_measures(conn, station_id, date):
   '''
   Delete measures for selected day and station
   :param conn: Connection object
   :param station_id: station id
   :param date: date in format (YYYY-MM-DD)
   '''
   query = measures.delete().where((measures.c.station_id == station_id) &
                                    (measures.c.date == datetime.strptime(date, '%Y-%m-%d').date()))
   conn.execute(query)
  
if __name__ == "__main__":

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
      all_stations = print_all_stations(conn)
      [print(s[0]) for s in all_stations]
      result = select_station_between_dates(conn, "USC00519397", "2010-01-01", "2010-01-31", "2010-01-15")
      #for row in result:
      #   print(row)
      update_measures_tobs(conn, "USC00519397", "2010-01-01", 65)
      delete_measures(conn, "USC00519397", "2010-01-02")

   with engine.connect() as conn:
      result = conn.execute("SELECT * FROM clean_stations LIMIT 5").fetchall()
      #print(result)
