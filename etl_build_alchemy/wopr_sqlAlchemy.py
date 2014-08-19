#Init dataset by sql alchemy

import sys
import os
import datetime

##====================== Map Reading ====================================##
#DATA_DIR = "/project/evtimov/wopr/data/"
#sqlDir = "../sql/init/"
#mapFile = '/project/evtimov/wopr/wopr-etl/etl_maps/chicago_311_street_lights_all_out.map'
#fp = open(mapFile, 'r')
#lines = fp.read().splitlines()

#datasetName = lines[1]
#print(datasetName)
#key = lines[3]
#print(key)
#obsDate = lines[5]
#print(obsDate)
#obsTs = lines[7]
#print(obsTs)
#attributes = [x.split()[0].lower() for x in lines[9:len(lines)]]
#print(attributes)
#types = [x.split()[1].upper() for x in lines[9:len(lines)]]
#print(types)

# assumes data file naming convention is: datasetname_yyyy-mm-dd.csv
#srcFiles = [f for f in os.listdir(DATA_DIR) if datasetName in f]
#srcFile = sorted(srcFiles, key=lambda f: f[-14:-4], reverse=True)[-1]
#srcFile = sorted(srcFiles, key=lambda f: f[-14:-4])[-2]
#print(srcFile)
#startDate = srcFile[-14:-4]
#datasetTag = datasetName.replace("-", "_")
# debug info
#print datasetTag, startDate

#create = [attributes[i] + " " + types[i] for i in range(0, len(attributes))]
#insert = [a for a in attributes]
#print(create)
#print(insert)


##========================Connect to Database===============================##

import os
import re
import psycopg2
from sqlalchemy import create_engine, types
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.pool import NullPool
from sqlalchemy.ext.declarative import declarative_base
from psycopg2.extensions import adapt, register_adapter, AsIs
import requests
import re
import os
from datetime import datetime, date, time
from urlparse import urlparse
from csvkit.unicsv import UnicodeCSVReader
from csvkit.typeinference import normalize_table
import gzip
from sqlalchemy import Boolean, Float, DateTime, Date, Time, String, Column, \
    Integer, Table, text, func, select, or_, and_
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.exc import NoSuchTableError
from types import NoneType
import requests
import re
from unicodedata import normalize

from sqlalchemy import UniqueConstraint
from sqlalchemy import Sequence


class PlenarioETL(object):

    def __init__(self, meta, data_dir):
        for k,v in meta.items():
            setattr(self, k, v)
        ##Here we need dataset_name/dl_url/userdefined key

        #domain = urlparse(self.source_url).netloc
        #fourbyfour = self.source_url.split('/')[-1]
        #self.view_url = 'http://%s/api/views/%s' % (domain, fourbyfour)
        #self.dl_url = '%s/rows.csv?accessType=DOWNLOAD' % self.view_url
        self.data_dir = data_dir
            
        #Need to define self.view_url, self.dl_url, self.dataset_name 
    def _download_csv(self):
        r = requests.get(self.dl_url, stream=True)
        self.fpath = '%s/%s_%s.csv.gz' % (self.data_dir,
              self.dataset_name, datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
        with gzip.open(self.fpath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()

##=======================Create Data Table=====================================##
    def _get_or_create_data_table(self):
        # Step One: Make a table where the data will eventually live
        #try:
            
            #self.dat_table = Table('dat_%s' % self.dataset_name, Base.metadata,
            #    autoload=True, autoload_with=engine, extend_existing=True)
            #self.dat_table.drop(engine, checkfirst=True)        
        #except NoSuchTableError:
        #    pass
        conn = engine.connect() 
        conn.execute('DROP TABLE IF EXISTS dat_%s' % self.dataset_name)
        if 1:  
            has_nulls = {}
            with gzip.open(self.fpath, 'rb') as f:
                reader = UnicodeCSVReader(f)
                header = reader.next()
                col_types,col_vals = normalize_table(reader)
                for idx, col in enumerate(col_vals):
                    if None in col_vals:
                        has_nulls[header[idx]] = True
                    else:
                        has_nulls[header[idx]] = False
            cols = [
                Column('%s_row_id' % self.dataset_name, Integer, primary_key=True),
                Column('start_date', TIMESTAMP, server_default=text('CURRENT_TIMESTAMP')),
                Column('end_date', TIMESTAMP, server_default=text('NULL')),
                Column('current_flag', Boolean, server_default=text('TRUE')),
                Column('dup_ver', Integer),
            ]
            for col_name,d_type in zip(header, col_types):
                kwargs = {}
                if has_nulls[col_name]:
                    kwargs['nullable'] = True
                cols.append(Column(slugify(col_name), COL_TYPES[d_type], **kwargs))
            cols.append(UniqueConstraint(self.key, 'dup_ver',name = 'uix_1'))
            #HERE, force constraint of uniqueness of userdefined_key and dup_ver.
            
            self.dat_table = Table('dat_%s' % self.dataset_name, Base.metadata, *cols, extend_existing=True)
            self.dat_table.create(engine, checkfirst=True)
    def _make_src_table(self):
        # Step Four: Make a table with every unique record.
        cols = []
        skip_cols = ['%s_row_id' %self.dataset_name, 'start_date', 'end_date', 'current_flag', 'dup_ver']
        for col in self.dat_table.columns:
            if col.name not in skip_cols:
                kwargs = {}
                if col.server_default:
                    kwargs['server_default'] = col.server_default
                cols.append(Column(col.name, col.type, **kwargs))
        cols.append(Column('line_num', Integer ,primary_key = True))
        self.src_table = Table('src_%s' % self.dataset_name, Base.metadata,
                          *cols, extend_existing=True)
        self.src_table.drop(bind=engine, checkfirst=True)
        self.src_table.create(bind=engine)

    def _insert_src_data(self):
        # Step Two: Insert data directly from CSV
        cols = []
        skip_cols = ['line_num']
        #skip_cols = ['%s_row_id' %self.dataset_name, 'start_date', 'end_date', 'current_flag', 'dup_ver','line_num']
        names = [c.name for c in self.src_table.columns]
        print(names)
        copy_st = 'COPY src_%s (' % self.dataset_name
        for idx, name in enumerate(names):
            if name not in skip_cols:
                print(idx)
                if idx < len(names) - len(skip_cols) - 1:
                    copy_st += '%s, ' % name
                else:
                    copy_st += '%s)' % name
                print(name)
        else:
            copy_st += "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')"
        conn = engine.raw_connection()
        cursor = conn.cursor()
        print copy_st
        with gzip.open(self.fpath, 'rb') as f:
            cursor.copy_expert(copy_st, f)
        conn.commit()
        
    def _make_data_table(self):
        from sqlalchemy import over

        skip_cols = ['%s_row_id' %self.dataset_name,'end_date', 'current_flag']
        print(self.dat_table.columns.keys()) 
        skip_src_col = ['line_num']
        from_vals = []
        from_vals.append(self.start_date)
        from_vals.append(func.rank().over(partition_by = self.src_table.columns[self.key], order_by = self.src_table.columns['line_num'].desc()).label('dup_ver'))
        print(self.src_table.columns.keys())
        for c_src in test.src_table.columns.keys():
            if c_src not in skip_src_col:
                from_vals.append(c_src)
        sel = select(from_vals, from_obj = test.src_table)
        ins = self.dat_table.insert().from_select([c for c in self.dat_table.columns.keys() if c not in skip_cols], sel)
        print(ins)
        conn = engine.connect()
        conn.execute(ins)


    
def slugify(text, delim=u'_'):
    if text:
        punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.]+')
        result = []
        for word in punct_re.split(text.lower()):
            word = normalize('NFKD', word).encode('ascii', 'ignore')
            if word:
                result.append(word)
        return unicode(delim.join(result))
    else:
        return text

##=========================Download Database===================================##
COL_TYPES = {
    bool: Boolean,
    int: Integer,
    float: Float,
    datetime: TIMESTAMP,
    date: Date,
    time: Time,
    NoneType: String,
    unicode: String
}


if __name__ == "__main__":
    wopr_conn = 'postgresql://'+username + ':' + password + '@' + hostname + ':' + port + '/' + database
    app_engine = create_engine(wopr_conn, convert_unicode=True)
    task_engine = create_engine(
        wopr_conn,
        convert_unicode=True,
        poolclass=NullPool)
    session = scoped_session(sessionmaker(bind=app_engine,
                                          autocommit=False,
                                          autoflush=False))

    task_session = scoped_session(sessionmaker(bind=task_engine,
                                          autocommit=False,
                                          autoflush=False))
    Base = declarative_base()
    Base.query = session.query_property()

    session = task_session
    engine = task_engine
    

    #testObj = dict([('dl_url', 'https://data.cityofchicago.org/api/views/ijzp-q8t2/rows.csv?accessType=DOWNLOAD'),('dataset_name', 'chicago-crimes-all'),('key','id')])
    # Here the key is a user_defined key, normally it's defined in the map file.
    testObj =  dict([('dl_url', 'https://data.cityofchicago.org/api/views/mq3i-nnqe/rows.csv?accessType=DOWNLOAD'),('dataset_name', 'bus_stop_boardings'),('key','stop_id'),('start_date', '\'2014-08-05\'')])

    DATA_DIR = "/project/evtimov/wopr/data/"
    test = PlenarioETL(testObj,DATA_DIR)
    #test._download_csv()
    #test.fpath = '%s/chicago_building_permits_2014-08-05T15:34:34.csv.gz' % test.data_dir


    test.fpath = '%s/bus_stop_boardings_2014-08-05T16:21:04.csv.gz' % test.data_dir
    ## This step is create data_table if not exist.
    test._get_or_create_data_table()
    
    print(test.dat_table.columns)
    ## This step is to create a src table if not exist.
    test._make_src_table()

    print(test.src_table.columns)

    ## This step is to insert into src table from local .csv file
    test._insert_src_data()

    ## This step is to insert into dat_table from src_table
        
    test._make_data_table()






















