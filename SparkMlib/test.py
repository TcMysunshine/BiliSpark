import glob
import os
import numpy as np
from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans,KMeansModel
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
import os
import tempfile
import math
from shutil import rmtree
from sklearn import metrics
import pandas as pd
import json
from SparkStreaming.MongoDB import MongoDB
# import matplotlib as plt
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
os.environ['PYSPARK_PYTHON'] = '/Users/chenhao/anaconda3/bin/python'

scf = SparkConf().setAppName("logo_cluster").setMaster("local[*]")
sc = SparkContext(conf=scf)
clusternum = 60
result_path = "/Users/chenhao/Desktop/BiliSpark/result/cluster-"+str(clusternum)
if not os.path.exists(result_path):
    os.mkdir(result_path)


def get_feature_list(feature_path):
    feature_list = []
    name_list = []
    # i = 0
    for feature_filepath in glob.glob(os.path.join(feature_path, "*.txt")):
        # if i < 8000:
        r_index = feature_filepath.rindex("/")
        file_name = feature_filepath[r_index + 1:]
        # print(file_name)
        name_list.append(file_name)
        with open(feature_filepath, "r") as r:
            temp_feature = r.read()
            temp_feature_list = [float(x) for x in temp_feature.split(",")]
        feature_list.append(temp_feature_list)
        # i += 1
    return feature_list, name_list

# '''高院图片聚类'''


# def gyFeatureCluster(clusternum):
#     sfz_feature_path = "/Users/chenhao/Documents/Data/SparkImage/imageData/身份证件"
#     yyzz_feature_path = "/Users/chenhao/Documents/Data/SparkImage/imageData/营业执照"
#     ems_feature_path = "/Users/chenhao/Documents/Data/SparkImage/imageData/EMS"
#     sfz_feature_list, sfz_name_list = get_feature_list(sfz_feature_path)
#     yyzz_feature_list, yyzz_name_list = get_feature_list(yyzz_feature_path)
#     ems_feature_list, ems_name_list = get_feature_list(ems_feature_path)
#     # print(type(sfz_feature_list))
#     # print(len(sfz_feature_list))
#     # print(sfz_feature_list[0])
#     # print(type(sfz_feature_list[0]))
#     train_feature_list = sfz_feature_list + yyzz_feature_list + ems_feature_list
#     train_name_list = sfz_name_list + yyzz_name_list + ems_name_list
#     # sfz_rdd = sc.parallelize(sfz_feature_list)
#     '''训练'''
#     model = KMeans.train(sc.parallelize(train_feature_list), clusternum, maxIterations=10,
#                          initializationMode="random", seed=50,
#                          initializationSteps=5, epsilon=1e-4)
#     model_path = tempfile.mkdtemp()
#     model.save(sc, model_path)
#
#     model = KMeansModel.load(sc, model_path)
#     predict = model.predict(sc.parallelize(train_feature_list))
#     print(predict.collect())
#     try:
#         rmtree(model_path)
#     except OSError:
#         pass
#     gy_result_path = "/Users/chenhao/Desktop/BiliSpark/result/gy_image_cluster.txt"
#     writeResultTofile(gy_result_path, train_name_list, predict.collect())


'''logo图片聚类'''


def logo_feature_cluster(train_feature_list, train_name_list, clusternum):
    '''训练'''
    model = KMeans.train(sc.parallelize(train_feature_list), clusternum, maxIterations=10,
                         initializationMode="random", seed=50,
                         initializationSteps=5, epsilon=1e-4)
    model_path = tempfile.mkdtemp()
    model.save(sc, model_path)
    model = KMeansModel.load(sc, model_path)
    '''预测'''
    predict = model.predict(sc.parallelize(train_feature_list))
    # print(predict.collect())
    try:
        rmtree(model_path)
    except OSError:
        pass

    logo_result_path = os.path.join(result_path,"logo_image_result"+str(clusternum)+".txt")
    writeResultTofile(logo_result_path, train_name_list, predict.collect())

    '''Calinski-Harabasz聚类评估指标'''
    # evaluationCH = metrics.calinski_harabaz_score(train_feature_list, predict.collect())
    # ch = str(round(evaluationCH, 2))
    # print("Calinski-Harabasz聚类评估指标:"+ch)
    # with open(result_path + "Calinski-Harabasz.txt", 'a') as a:
    #     a.write(str(clusternum)+":" + ch + "\n")

    '''Silhouette-Coefficient聚类评估指标'''
    # evaluationSS = metrics.silhouette_score(train_feature_list, predict.collect(), metric='euclidean')
    # ss = str(round(evaluationSS, 3))
    # print("Silhouette-Coefficient聚类评估指标:" + ss)
    # with open(result_path + "Silhouette-Coefficient.txt", 'a') as a:
    #     a.write(str(clusternum) + ":" + ss + "\n")
    # print())


def writeResultTofile(filepath,name_list,result):
    if os.path.exists(filepath):
        os.remove(filepath)
    with open(filepath, "a") as a:
        for i in range(len(name_list)):
            # print(str(name_list[i]) + ":" + str(result[i]))
            a.write(str(name_list[i]) + ":" + str(result[i]) + "\n")


def getLabelImage(filepath,cluster_num):
    labelDic={}
    for i in range(cluster_num):
        labelDic[str(i)] = []
    # labelList
    with open(filepath, "r") as r:
        lines = r.readlines()
        for tempLine in lines:
            linesList = tempLine.strip().split(":")
            filename = linesList[0]
            label = linesList[1]
            labelDic[label].append(filename)
    return labelDic


if __name__ == "__main__":
    # gyFeatureCluster(3)

    # for cnum in range(10, 20, 1):
    #     clusternum = cnum
    #     '''开始训练预测'''
    #     logo_feature_path = "/Users/chenhao/Documents/Data/SparkImage/SY/feature"
    #     logo_train_feature_list, logo_name_list = get_feature_list(logo_feature_path)
    #     # print(len(logo_name_list))
    #     logo_feature_cluster(logo_train_feature_list, logo_name_list, clusternum)
    #

    '''开始训练预测'''
    logo_feature_path = "/Users/chenhao/Documents/Data/SparkImage/SY/feature"
    logo_train_feature_list, logo_name_list = get_feature_list(logo_feature_path)
    # print(len(logo_name_list))
    logo_feature_cluster(logo_train_feature_list, logo_name_list, clusternum)
    '''获取每一类的个数'''
    image_cluster_path = os.path.join(result_path, "logo_image_result" + str(clusternum) + ".txt")
    labelDict = getLabelImage(image_cluster_path, clusternum)
    with open(os.path.join(result_path, "logo_image_count" + str(clusternum) + ".txt"), 'a') as a:
        for i in range(clusternum):
            # print(len(labelDict.get(str(i))))
            a.write(str(i)+":"+str(len(labelDict.get(str(i))))+"\n")
    '''画图'''

    # label = '5'
    # image_count = len(labelDict.get(label))
    # # width, height = 75, 75
    # nb_rows = int(math.ceil(float(image_count) / 5))
    # nb_cols = 6
    # plt.figure(figsize=(15, 15))
    # print("行数")
    # print(nb_rows)
    # for k in range(image_count):
    #     image_name = labelDict.get(label)[k]
    #     dotindex = image_name.rindex(".")
    #     image_filepath = "/Users/chenhao/Documents/Data/SparkImage/SY/image/"
    #     image_path = image_filepath + image_name[0:dotindex]
    #     showImage = mpimg.imread(image_path)
    #     plt.subplot(nb_rows, nb_cols, k + 1)
    #     plt.imshow(showImage)
    #     plt.axis('off')
    # # plt.savefig(result_path + "label-" + label + ".png")
    # plt.show()

    for i in range(clusternum):
        label = str(i)
        # image_count = labelDict.get(label)[k]
        image_filepath = "/Users/chenhao/Documents/Data/SparkImage/SY/image/"
        nb_cols = 5
        plt.figure(figsize=(15, 15))
        image_count = len(labelDict.get(label))
        nb_rows_temp = int(math.ceil(float(image_count) / 5))
        if nb_rows_temp > 10:
            nb_rows = 10
        else:
            nb_rows = nb_rows_temp
        for k in range(nb_rows * nb_cols):
            if k<image_count:
                image_name = labelDict.get(label)[k]
                dotindex = image_name.rindex(".")
                image_path = image_filepath + image_name[0:dotindex]
                showImage = mpimg.imread(image_path)
                plt.subplot(nb_rows, nb_cols, k + 1)
                plt.imshow(showImage)
                plt.axis('off')
            else:
                break
        plt.savefig(os.path.join(result_path, "label-" + label + ".png"))
        # plt.show()
