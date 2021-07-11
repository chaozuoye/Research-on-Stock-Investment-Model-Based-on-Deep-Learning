import time
import pymysql
from guba import DataOutput
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker
plt.rcParams['font.sans-serif'] =['SimHei']
plt.rcParams['axes.unicode_minus']=False
d = DataOutput()


class Analyse():
    def __init__(self):
        self.host = "localhost"
        self.db = 'guba'
        self.user = 'root'
        self.passwd = 'LH001103'
        self.score=[]

    # 该部分计算每个title的情绪得分
    def get_score_and_polarity(self):
        db = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database=self.db)
        cursor = db.cursor()
        sql01 = 'select titles,id from info_guba'
        cursor.execute(sql01)
        res = cursor.fetchall()
        for i in res:
            # 使用snownlp计算情绪值
            score_snownlp = d.feeling(i[0])
            self.score.append(score_snownlp)
            # 评出情感极性 将情绪得分>0.6的评论当作积极评论，小于0.4的评论当作消极评论。
            if score_snownlp <= 0.6:
                if score_snownlp > 0.4:
                    p = 0
                else:
                    p = -1
            else:
                p = 1
            sql02 = "update info_guba set f_scores={0},polarity={1} where id={2}".format(score_snownlp, p, i[1])
            cursor.execute(sql02)
            db.commit()
        # db.commit()
        db.close()

    # 计算单股吧一天内所有帖子情绪值加和求平均
    def get_score_of_day(self):
        db = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database=self.db)
        cursor = db.cursor()
        sql03 = "select company_name from info_guba group by company_name"
        cursor.execute(sql03)
        com_names = cursor.fetchall()
        score=[]
        day=[]
        for i in com_names:
            # 该语句查询每只股票散户每天发贴的平均情绪值
            # sql04 = "select round(sum(f_scores)/count(f_scores),10),mdates,dates from info_guba " \
            #         "where company_name='{0}' and usernames not like '%资讯%' group by " \
            #         "mdates order by mdates".format(i[0])

            # 该语句查询每只股票散户每天发贴的总情绪值
            sql04 = "select round(sum(f_scores),10),mdates,dates from info_guba " \
                    "where company_name='{0}' and usernames not like '%资讯%' group by " \
                    "mdates order by mdates".format(i[0])
            cursor.execute(sql04)
            res = cursor.fetchall()
            #print(res,len(res))
            for j in res:
                score.append(j[0])
                day.append(j[2])
        db.close()
        return score,day

    # 将日期格式转换为时间戳
    def date_to_timestamp(self, year, month):
        mdate = str(year) + '-' + str(month)
        time_array = time.strptime(mdate, "%Y-%m")
        news_timestamp = time.mktime(time_array)
        return news_timestamp

if __name__=='__main__':
    A=Analyse()
    #A.get_score_and_polarity()
    #获取数据
    stock_code='600519'
    scores,days=A.get_score_of_day()
    df = pd.read_csv(stock_code + '.csv')
    close=df['close'].values.tolist()
    kdate=df['date'].values.tolist()    #股市开市时间
    newclose=[]
    newscores=[]
    index=0
    times = pd.date_range(start=kdate[0], end=kdate[-1], freq='D')
    date_list = [time.strftime('%Y-%m-%d') for time in times]
    #将未开盘的股价设置为上一交易日的股价
    # （正常情况下应该是每天都有帖子发布，
    # 但在实际爬取过程中发现2020年8月13日没有帖子发布）
    for i in range(len(date_list)):
        newscores.append((scores[index]))
        if date_list[i]==days[index+3]: #情绪值的日期是从2020-07-07开始，而股价是07-10开始
            index+=1
    index=0
    for i in range(len(date_list)):
        newclose.append(close[index])
        if date_list[i]==kdate[index]:
            index+=1
    fig=plt.figure()
    ax1=fig.add_subplot(111)
    ax1.plot(date_list,newclose,color='blue',label='日收盘价')
    ax1.set_ylim(0,max(close))
    ax1.legend(loc='lower right', bbox_to_anchor=(0.4, -0.3))
    ax2=ax1.twinx()
    plt.plot(date_list,newscores,color='red',label='日平均情绪值（右）')
    plt.legend(loc='lower left', bbox_to_anchor=(0.4, -0.3))

    ax1.xaxis.set_major_locator(ticker.MultipleLocator(base=30))
    fig.autofmt_xdate()
    plt.title('贵州茅台')

    plt.savefig('收盘价与平均情绪值.png')
    plt.show()

    #计算相关性
    data=pd.DataFrame({'close':newclose[1:],
                       'score':newscores[:-1]})
    print("pearson相关系数：\n",data.corr('pearson'))  #针对线性
    # print("kendall相关系数：\n",data.corr('kendall'))   #针对无序
    print("spearman相关系数：\n",data.corr('spearman')) #针对非线性
