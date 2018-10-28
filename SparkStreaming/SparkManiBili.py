from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
import os
import pandas as pd
import json
from SparkStreaming.MongoDB import MongoDB
os.environ['PYSPARK_PYTHON'] = '/Users/chenhao/anaconda3/bin/python'


class BiliSparkStreaming():
    def __init__(self, master):
        self.master = master
        scf = SparkConf().setAppName("BiliSpark").setMaster(self.master).set("spark.cores.max", "3")
        self.sc = SparkContext(conf=scf)
        # sc.setLogLevel(logging.WARNING)
        '''监控文件目录'''
        self.monitor_directory = "/Users/chenhao/Documents/BiliSpark/data"
        '''写入文件目录'''
        self.writeDirectory = '/Users/chenhao/Documents/BiliData'
        self.streamingContext = StreamingContext(self.sc, 10)
        sparkSession = SparkSession.builder.config(conf=scf).getOrCreate()

        self.mongo = MongoDB("mongodb://localhost:27017/", "bili")

        self.months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        '''存储目前为止所有的数据'''
        self.total_data = None

    '''获取SparkSession实例'''
    def getSparkSessionInstance(self, sparkConf):
        if ('sparkSessionSingletonInstance' not in globals()):
            globals()['sparkSessionSingletonInstance'] = SparkSession \
                .builder \
                .config(conf=sparkConf) \
                .getOrCreate()
        return globals()['sparkSessionSingletonInstance']
    '''监控并处理数据'''
    def monitor_process_data(self):
        bili_data = self.streamingContext.textFileStream(self.monitor_directory)
        bili_data = bili_data.map(lambda line: (line.split(",")[1],
                                                line.split(",")[2],
                                                int(line.split(",")[3]),
                                                int(line.split(",")[4]),
                                                line.split(",")[5]
                                  ))
        bili_data.foreachRDD(self.process_data)
        self.streamingContext.start()
        self.streamingContext.awaitTerminationOrTimeout(2000)
    '''每一个rdd进行处理数据'''
    def process_data(self, time, rdd):
        print("时间:" + str(time))
        if not rdd.isEmpty():
            try:
                spark = self.getSparkSessionInstance(rdd.context.getConf())
                rowRDD = rdd.map(lambda x: Row(tv=x[0], label=x[1], play=x[2], dm=x[3], month=x[4]))
                '''转换为DataFrame'''
                temp_bili_data = spark.createDataFrame(rowRDD)
                temp_bili_data.show()
                '''目前为止所有的数据'''
                if self.total_data is None:
                    self.total_data = temp_bili_data.toPandas()
                else:
                    self.total_data = pd.concat([self.total_data, temp_bili_data.toPandas()], axis=0, sort=True,
                                                ignore_index=True)
            except BaseException as e:
                print(e)
        '''记录目前为止所有的数据'''
        if self.total_data is not None:
            '''根据番剧计算播放量和弹幕量  所有月份的'''
            tv_bili = self.total_data.groupby(['tv']).sum().reset_index()

            '''将所有月的播放量和弹幕存储到MongoDB中'''
            # tv_bili_json = self.db2Json(tv_bili)
            #
            # for tv_bili in tv_bili_json:
            #     # print(tv_bili)
            #     query = {
            #         'tv': tv_bili['tv']
            #     }
            #     # print(query)
            #     self.insert_mongo('allmonth_play_dm', query, tv_bili)
            # """
            '''每个月各个番剧的播放量或者弹幕并排序'''
            for tempmonth in self.months:
                '''筛选出各个月份的播放量和弹幕'''
                temp_month_tv = self.total_data[self.total_data['month'] == tempmonth]
            #     '''根据番剧名字进行叠加'''
                temp_month_tv_bymonth = temp_month_tv.groupby(["tv"]).sum().reset_index()
                if not temp_month_tv.empty:
                    '''由于groupby时将月份去除了，因此要重新加上这一列'''
                    temp_month_tv_bymonth['month'] = tempmonth
                    temp_month_tv_bymonth_json = self.db2Json(temp_month_tv_bymonth)
                    print(temp_month_tv_bymonth_json)
                    for x in temp_month_tv_bymonth_json:
                        query = {
                            'tv': x['tv'],
                            'month': tempmonth #月份查询条件不要忘记
                        }
                        # print(x)
                        self.insert_mongo('bymonth_play_dm', query, x)
            #     if not temp_month_tv.empty:
            #         '''各个月份的番剧的播放量'''
            #         temp_month_tv_sort = temp_month_tv_bymonth.sort_values(by='play', ascending=False)
            #         self.write_playOrdm_bymonth(temp_month_tv_sort, tempmonth, 'play')
            #     if not temp_month_tv.empty:
            #         '''各个月份的番剧的弹幕'''
            #         temp_month_dm_sort = temp_month_tv_bymonth.sort_values(by='dm', ascending=False)
            #         self.write_playOrdm_bymonth(temp_month_dm_sort, tempmonth, 'dm')
            # self.write_playOrdm_Allmonth(tv_bili, 'play')
            # self.write_playOrdm_Allmonth(tv_bili, 'dm')
            # """

    '''datafrmae转换为json数据'''
    def db2Json(self, db):
        json_record = db.to_json(orient='records')
        return json.loads(json_record)

    '''根据月份统计各个番剧的播放量或者弹幕数'''
    def write_playOrdm_bymonth(self, data, month, dmorplay):
        filepath = self.writeDirectory + month + '月-' + dmorplay + ".txt"
        row = data.shape[0]
        with open(filepath, 'w') as w:
            for i in range(row):
                tempdata = data.iloc[i]
                # print(data[key] + str(data[value]))
                w.write(tempdata['tv'] + ":" + str(tempdata[dmorplay]) + "\n")

    '''将各个番剧的播放量或者弹幕数写入文件'''
    def write_playOrdm_Allmonth(self, data, dmorPlay):
        filepath = self.writeDirectory + 'tv' + "- " + dmorPlay + ".txt"
        row = data.shape[0]
        with open(filepath, 'w') as w:
            for i in range(row):
                tempdata = data.iloc[i]
                # print(data[key] + str(data[value]))
                w.write(tempdata['tv'] + ":" + str(tempdata[dmorPlay]) + "\n")

    '''将数据插入数据库'''
    def insert_mongo(self, collection, query, data):
        self.mongo.insertOrUpdate(collection, query, data)


if __name__ == '__main__':
    master = "local[*]"
    bss = BiliSparkStreaming(master)
    bss.monitor_process_data()