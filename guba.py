import re
import time
import jieba
import pymysql
import requests
import pandas as pd
from lxml import etree
from snownlp import SnowNLP


class guba():
    def __init__(self, host, db, user, passwd):
        self.headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, '
                                      'like Gecko) Chrome/89.0.4389.90 Safari/537.36 Edg/89.0.774.54'}
        self.host = host
        self.db = db
        self.user = user
        self.passwd = passwd
        self.dataoutput = DataOutput()
        self.ip_num = 1

    # 移除换行符
    def rm_special_letters(self, old_list):
        new_list = []
        for i in old_list:
            i = i.replace('\r\n', '')
            i = i.replace(' ', '')
            new_list.append(i)
        return new_list

    # 将日期格式转换为时间戳
    def date_to_timestamp(self, year, timestr):
        mdate = str(year) + '-' + timestr
        time_array = time.strptime(mdate, "%Y-%m-%d")
        news_timestamp = time.mktime(time_array)
        return news_timestamp

    # 获取每日热帖  阅读量 评论量 标题 用户 发帖时间
    def dangu_pinglun(self, url, company_name, industry):
        """
        :param 所属板块:
        :param 公司名称:
        :type url: 股吧首页链接
        """
        global mtimestamp
        mtimestamp = time.time()
        page = 1
        year = 2021
        latest_mounth = 7
        while True:
            datalist = []
            try:
                # 拼接url
                murl = url + str(page) + '.html'
                resp = requests.get(murl, headers=self.headers,timeout=10)
                #print(resp.text)
                htmltree = etree.HTML(resp.text)
                yuedu_count = htmltree.xpath('//span[@class="l1 a1"]/text()')
                yuedu_count = self.rm_special_letters(yuedu_count)[1:]
                pinglun_count = htmltree.xpath('//span[@class="l2 a2"]/text()')
                pinglun_count = self.rm_special_letters(pinglun_count)[1:]
                title_list = htmltree.xpath('//span[@class="l3 a3"]/a/@title')
                username_list = htmltree.xpath('//span[@class="l4 a4"]/a//text()')
                last_time_list = htmltree.xpath('//span[@class="l5 a5"]/text()')[1:]
                # 此处将评论列表保存到字典 交给dataoutput储存
                for num, p in enumerate(pinglun_count):
                    # 当阅读数量含有汉字时
                    if re.search('[\u4e00-\u9fa5]', yuedu_count[num]):
                        yuedu_count[num] = 20000
                    if re.search('[\u4e00-\u9fa5]', pinglun_count[num]):
                        pinglun_count[num] = 20000
                    # 截取时间具体提到天 去掉时分时间
                    lastdate = last_time_list[num].split(' ')[0]
                    # 发帖时间递减 ，当下层月份大于上边时年份减一
                    if int(lastdate.split('-')[0]) > latest_mounth:
                        year -= 1
                    mtimestamp = self.date_to_timestamp(year, lastdate)
                    info_dict = {'scan': yuedu_count[num],
                                 'comment_num': pinglun_count[num],
                                 'title': title_list[num],
                                 'username': username_list[num],
                                 'mdate': mtimestamp,
                                 'dates':str(year)+'-'+lastdate,
                                 'company': company_name,
                                 'industry': industry}
                    datalist.append(info_dict)
                    latest_mounth = int(lastdate.split('-')[0])
                page += 1

                # 将存库语句写到这里是为了个别字节数据储存终端而导致总程序终段
                self.dataoutput.write_to_mysql(host=self.host, db=self.db, user=self.user, passwd=self.passwd,
                                               datalist=datalist)
                time.sleep(1)
            except Exception as e:
                print(industry, company_name, page, "---" + str(time.time()))
                print(str(e))
                if 'HTTPConnectionPool' in str(e):
                    print("ip被ban")
                if 'index out of range' in str(e):
                    page += 1
                elif 'day is out of range for month' in str(e):
                    page += 1
            # 此处判断总时间是否到达最大时间  即到2019年1月终止 爬取下一个
            if mtimestamp <=1594310400:             # 1521475200:
                print('时间到')
                break


class DataOutput():
    def __init__(self):
        self.__tablename = 'info_guba'
        self.__tablekeys = '(myid,scans,comments,titles,usernames,mdates,dates,f_scores,company_name,industry)'

    # 删除特殊字符 以防引起mysql异常
    def rm_special_letter(self, line):
        for i in ["\'", "\"", "#", "\\"]:
            line = line.replace(i, "")
        return line

        """借助snownlp
        分析news的情绪分为3级 0：积极  1：中立  2：消极"""

    def feeling(self, line):
        try:
            res = SnowNLP(line)
            f_score = res.sentiments
        except:
            f_score = 0
        return f_score

    def __rm_stopwords(self, wordlist):
        new_wordlist = []
        with open('tool_files/stopwords.txt', 'r', encoding='utf-8') as r:
            stopwords = r.read()
            for i in wordlist:
                if i in stopwords:
                    continue
                else:
                    new_wordlist.append(i)
            return new_wordlist

        """使用玻森情感词典 计算情绪指数"""

    #TODO：待修改内容
    def feeling2(self, line):
        path = "tool_files/sentiment_score.txt"
        df = pd.read_table(path, sep=" ", names=['key', 'score_snownlp'])
        key = df['key'].values.tolist()
        score = df['score_snownlp'].values.tolist()

        def getscore(line):
            segs = jieba.lcut(line)  # 分词
            jieba.load_userdict('tool_files/userdict.txt')
            segs = self.__rm_stopwords(segs)
            score_list = [score[key.index(x)] for x in segs if (x in key)]
            # 修改后的sentiment_score.txt 得分有的为字符串格式不能直接使用sum求和
            # print(score_list)
            if len(score_list) != 0:
                sums = 0
                for i in score_list:
                    sums = sums + float(i)
                return sums / len(score_list)  # 计算得分
            else:
                return 0

        last_score = getscore(line)
        if last_score == 0:
            return 0
        else:
            return round(last_score, 5)

    # 数据去重
    def __mysql_data_rechecking(self, item, ids_inmysql):
        id_inmysqls = [myid[0] for myid in ids_inmysql]
        title = self.rm_special_letter(item['title'])
        myid = item['username'] + str(item['mdate'])[3:-4] + title[:100]
        if myid not in id_inmysqls:
            return 'newrecord', title, myid
        else:
            return '数据已存在'

    def write_to_mysql(self, datalist, host, db, user, passwd):
        # mysql连接初始化连接
        db = pymysql.connect(host=host, user=user, password=passwd, database=db)
        # 使用 cursor() 方法创建一个游标对象cursor
        cursor = db.cursor()
        # 查询表中 plantform title username 数据拼接字符串用于去重
        quchong_sql = 'SELECT myid FROM {}'.format(self.__tablename)
        cursor.execute(quchong_sql)
        myids = cursor.fetchall()
        for item in datalist:
            data = self.__mysql_data_rechecking(item, myids)
            if data[0] == 'newrecord':
                title, myid = data[1], data[2]
                # feeling = self.feeling(title)
                feeling = 0
                # SQL插入语句
                sql = "INSERT INTO {TABLENAME}{keys}" \
                      "VALUES ('{v0}','{v1}','{v2}','{v3}','{v4}','{v5}','{v6}','{v7}','{v8}','{v9}')".format \
                    (TABLENAME=self.__tablename,
                     keys=self.__tablekeys,
                     v0=myid,
                     v1=item['scan'],
                     v2=item['comment_num'],
                     v3=title,
                     v4=item['username'],
                     v5=item['mdate'],
                     v6=item['dates'],
                     v7=feeling,
                     v8=item['company'],
                     v9=item['industry'])
                try:
                    # 执行sql语句
                    cursor.execute(sql)
                    # 执行sql语句
                    db.commit()
                except Exception as e:
                    if 'PRIMARY' in str(e):
                        print('查重失败')
                    else:
                        print(item)
                        print(str(e) + "---" + str(time.time()))
                        # 发生错误时回滚
                        db.rollback()
                        # raise e
        # 关闭数据库连接
        db.close()



if __name__ == '__main__':
    gb = guba(host='localhost', db='guba', user='root', passwd='LH001103')

    stock_code ="600519"
    name ="贵州茅台"
    url = 'http://guba.eastmoney.com/list,' + str(stock_code) + ',99,f_'
    # print(url)
    #print(gb.date_to_timestamp(2020,"07-10"))
    gb.dangu_pinglun(url, name, "酿酒")