import sqlalchemy as sqla
import pymysql
import pandas as pd
class write_to_file():
    def __init__(self,host,db,user,password):
        self.host = host
        self.db = db
        self.user = user
        self.password = password
    def dataToTxt(self):
        db=pymysql.connect(host=self.host, user=self.user, password=self.password, database=self.db)
        cur=db.cursor()
        cur.execute("SELECT titles FROM info_guba_01 where  polarity=-1")
        Strings=cur.fetchall()
        with open('../output/neg.txt', 'w',encoding='utf-8') as f:  # 设置文件对象
            for Str in Strings:
                f.write("".join(Str)+"\n")  # 将字符串写入文件中
        print("写入完成")


if __name__ == "__main__":
    host,tablename,user,password='127.0.0.1','guba.info_guba','root','LH001103'
    test=write_to_file(host=host,user=user,db='guba',password=password)
    test.dataToTxt()