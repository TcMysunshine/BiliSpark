from pyspark.sql import SparkSession
from pyspark import SparkConf
import os,threading, time
os.environ['PYSPARK_PYTHON'] = '/Users/chenhao/anaconda3/bin/python'


class SimuStreamThread(threading.Thread):
    '''模拟写入一个文件线程'''
    def __init__(self, data, write_directory):
        #总是忘记写父类的初始化
        super(SimuStreamThread, self).__init__()
        self.data = data
        '''数据需要写入的文件夹'''
        self.write_directory = write_directory

    def run(self):
        i = 0
        length = len(self.data)
        print("数据长度"+str(length))
        #每次读取五十条数据
        while i < length:
            start = i
            end = i + 50
            if end > length:
                data = self.data[start:length]
            else:
                data = self.data[start:end]
            timestamp = str(int(time.time()))
            '''时间戳命名文件'''
            filepath = self.write_directory + "/bili" + timestamp + ".txt"
            with open(filepath, 'a') as a:
                for text in data:
                    a.write(text + "\n")
            i = end
            time.sleep(1)
            print("睡眠一秒" + str(i))

'''读数据'''
def readData(filepath):
    linesList = []
    with open(filepath, 'r') as r:
        lines = r.readlines()
        for line in lines:
            line = line.strip()
            print(line)
            linesList.append(line)
    return linesList


if __name__ == '__main__':
    read_filepath = "/Users/chenhao/Documents/BiliData/bili.csv"
    write_directory = "/Users/chenhao/Documents/BiliSpark/data"
    data = readData(read_filepath)
    sst = SimuStreamThread(data, write_directory)
    sst.start()