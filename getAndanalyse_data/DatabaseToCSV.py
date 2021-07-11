import re
import time
import jieba
import pymysql
import requests
import pandas as pd
from lxml import etree
from snownlp import SnowNLP
import sqlalchemy as sqla
import pandas as pd
class translate():
    def __init__(self,host,db,user,password,tablename):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.tablename = tablename
    def dataToCsv(self):
        db=sqla.create_engine('mysql+pymysql://root:LH001103@localhost:3306/guba?charset=utf8')
        df=pd.read_sql('select * from info_guba',db)
        print(df)
        df.to_csv('test.csv',encoding="ansi")   #存为ANSI格式主要是防止打开csv文件乱码
if __name__ == "__main__":
    host,tablename,user,password='127.0.0.1','guba.info_guba','root','LH001103'
    test=translate(host=host,tablename=tablename,user=user,db='guba',password=password)
    test.dataToCsv()


