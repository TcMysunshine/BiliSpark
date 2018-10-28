from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
import os
import pandas as pd
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

        self.streamingContext = StreamingContext(self.sc, 10)
        sparkSession = SparkSession.builder.config(conf=scf).getOrCreate()

        self.mongo = MongoDB("mongodb://localhost:27017/", "bili")

        self.months = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10']

        self.tv_play_df = None
        self.tv_dm_df = None
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
                '''番名，播放量'''
                if self.tv_play_df is None:
                    self.tv_play_df = temp_bili_data.toPandas()
                else:
                    self.tv_play_df = pd.concat([self.tv_play_df, temp_bili_data.toPandas()], axis=0, sort=True,
                                                ignore_index=True)
                '''番名，弹幕'''
                if self.tv_dm_df is None:
                    self.tv_dm_df = temp_bili_data.toPandas()
                else:
                    self.tv_dm_df = pd.concat([self.tv_dm_df, temp_bili_data.toPandas()], axis=0, sort=True,
                                              ignore_index=True)

            except BaseException as e:
                print(e)
        '''记录目前为止所有的数据'''
        if self.tv_play_df is not None:
            key = 'tv'
            value = 'play'
            allmonth_tv_play_df = self.get_groupbySum_type_data(self.tv_play_df, key)
            '''每个月各个番剧的弹幕数并排序'''
            for tempmonth in self.months:
                temp_month_tv_play = self.tv_play_df[self.tv_play_df['month'] == tempmonth]
                if not temp_month_tv_play.empty:
                    temp_month_tv_sort = temp_month_tv_play.groupby(["tv"]).sum().reset_index().sort_values(by='play', ascending=False)
                    self.write_playOrdm_bymonth(temp_month_tv_sort, tempmonth, value)
            # self.writeFile(allmonth_tv_play_df, key, value)
            # self.insert_mongo(key+"_"+value, key, self.tv_play_df)
        if self.tv_dm_df is not None:
            key = 'tv'
            value = 'dm'
            '''每个月各个番剧的弹幕数并排序'''
            allmonth_tv_dm_df = self.get_groupbySum_type_data(self.tv_dm_df, key)
            for tempmonth in self.months:
                temp_month_tv_play = self.tv_play_df[self.tv_play_df['month'] == tempmonth]
                if not temp_month_tv_play.empty:
                    temp_month_dm_sort = temp_month_tv_play.groupby(["tv"]).sum().reset_index().sort_values(by='dm', ascending=False)
                    self.write_playOrdm_bymonth(temp_month_dm_sort, tempmonth, value)

            # self.writeFile(allmonth_tv_dm_df, key, value)
        # 根据key值分组然后统计数据

    '''根据月份统计各个番剧的播放量或者弹幕数'''
    def write_playOrdm_bymonth(self, data, month, value):
        filepath = "../result/" + month + '月-' + value + ".txt"
        row = data.shape[0]
        with open(filepath, 'w') as w:
            for i in range(row):
                tempdata = data.iloc[i]
                # print(data[key] + str(data[value]))
                w.write(tempdata['tv'] + ":" + str(tempdata[value]) + "\n")

    '''统计所有时间段中各个番剧的总播放量和弹幕数'''
    def get_groupbySum_type_data(self, data, key):
        result = data.groupby([key]).sum().reset_index()
        return result

    # def insert_mongo(self, db, key, data):
    #     row = data.shape[0]
    #     print(row)
    #     data = data.to_json(orient='index')
    #     print(type(data))
    #     for i in range(row):
    #         tempdata = data[i]
    #         print(tempdata)
    #         value = tempdata[key]
    #         query = {
    #             key: value
    #         }
    #         self.mongo.insertOrUpdate(db, query, data)
    '''根据要取得键值对写入文件'''
    def writeFile(self, data, key, value):
        filepath = key + "- " + value + ".txt"
        row = data.shape[0]
        with open(filepath, 'w') as w:
            for i in range(row):
                tempdata = data.iloc[i]
                # print(data[key] + str(data[value]))
                w.write(tempdata[key] + ":" + str(tempdata[value]) + "\n")


if __name__ == '__main__':
    master = "local[*]"
    bss = BiliSparkStreaming(master)
    bss.monitor_process_data()