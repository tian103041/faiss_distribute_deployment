"""
/*
    本服务功能：
    1.接收业务端的请求并解析；
    2.解析后，将被查询向量与质心向量做相似计算，并返回TopK;
    3.发送特征向量到相应节点；
    4.接收slave返回的TopK；
    5.查询，映射回adid并返回;
*/
"""

from flask import Flask, jsonify, request
import numpy as np
import time
import os
import hdfs
import json
import requests

def get_centreids_data(path,path_centreids_vector):
    dict_centreids_vector = {}
    try:
        with path.read(path_centreids_vector) as reader:
            data_hdfs = reader.read()
            data_str_all = bytes.decode(data_hdfs)
            line_split_all = data_str_all.split("\n")
            len_split = len(line_split_all)
            line_split_every_data = line_split_all[0:len_split - 1]
            for line in line_split_every_data:
                line_replace = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("\n", "")
                line_split = line_replace.split(",")
                centreid = int(line_split[0])
                centreid_vector = []
                for i in range(1, len(line_split)):
                    centreid_vector.append(float(line_split[i]))
                dict_centreids_vector[centreid] = centreid_vector
    except IOError:
        print("Error: 质心及质心向量文件没有找到或读取失败")
    return dict_centreids_vector

def get_adid_index_data(path,path_adid_index):
    dict_adid_index = {}
    try:
        with path.read(path_adid_index) as reader:
            data_hdfs = reader.read()
            data_str_all = bytes.decode(data_hdfs)
            line_split_all = data_str_all.split("\n")
            for i in line_split_all:
                if i == '':
                    line_split_all.remove(i)
            for line in line_split_all:
                line_split = line.replace("(", "").replace(")", "").replace("g", "").replace("_", "").split(",")
                adid = line_split[0]
                adid_index = line_split[1]
                dict_adid_index[adid_index] = adid
    except IOError:
        print("Error: adid映射文件没有找到或读取失败")
    return dict_adid_index

def get_ip_path(path,ip_path):
    dict_ip_path = {}
    with path.read(path_centreids_vector) as csvfile:
        csv_reader = csvfile.read()
        byte_decode = bytes.decode(csv_reader)
        line_split = byte_decode.split("\r\n")
        for i in line_split:
            if i == " ":
                line_split.remove(i)
        for ip_path in line_split:
            ip_path_split = ip_path.split("\t")
            if len(ip_path_split) == 2:
                dict_ip_path[ip_path_split[0]] = ip_path_split[1]
    return dict_ip_path

now = time.strftime("%Y-%m-%d", time.localtime())  # 获取现在的日期
app = Flask(__name__)

@app.route("/faiss_service_of_wallpaper/", methods=['GET'])
def pku_seg1():
    try:
        start_all_service_time = time.time()

        #解析请求体,得到待查询向量
        start_query_analysis_time = time.time()
        # if request.method == 'POST':
        #     http_input = request.get_data()
        #     http_dict = json.loads(http_input)
        #     nprobe_input = http_dict['nprobe']
        #     top_n_input = http_dict['top_n']
        #     data_input = http_dict['data']
        # elif request.method == 'GET':
        nprobe_input = request.args.get('nprobe','1')
        top_n_input = request.args.get("top_n")
        data_input = request.args.get("data")
        data_string = data_input
        topk = int(top_n_input)
        nprobe = int(nprobe_input)
        end_query_analysis_time = time.time()
        print("请求体解析成功，获得待查询数据，nprobe和topk,所用时间：",end_query_analysis_time-start_query_analysis_time)

        start_get_dif_query_data_time = time.time()
        data_list = data_string.split(",")
        xq = np.array(data_list).astype('float32')
        end_get_dif_query_data_time = time.time()
        print("获取所有的请求数据，并生成二维数组成功，所用时间：",end_get_dif_query_data_time-start_get_dif_query_data_time)

        start_calculate_distance_time = time.time()
        dict_dis = {}
        #计算待查询向量与各个质心的距离
        for key in dict_centreids_vector:
            centre_array = np.array(dict_centreids_vector[key])
            distance = np.linalg.norm(xq-centre_array)
            dict_dis[key] = distance
        end_calculate_distance_time = time.time()
        print("计算待查询向量与聚类中心的距离成功，所用时间：",end_calculate_distance_time-start_calculate_distance_time)

        #根据距离排序,并返回前nprobe个质心
        start_sort_by_dis_time = time.time()
        list_dis = sorted(dict_dis.items(), key=lambda d: d[1])[0:nprobe]
        end_sort_by_dis_time = time.time()
        print("排序成功，所用时间：",end_sort_by_dis_time-start_sort_by_dis_time)

        #将待查询向量发送给相应slave,找到对应的从节点，然后将xq通过http发送出去,
        #获取slave返回结果，并将所有结果添加到数组中
        D_I_all_slave = []
        start_query_slave_time = time.time()
        for item in list_dis:
            # 返回值类型
            # {"status": "success",
            # "results": [[["10.111874", "15154"], ["10.380161", "8123"], ["10.475431", "41584"], ["10.484323", "70242"]]]}
            #slave = centreid_slave.get(item[0])
            #slave_return_json = requests.get("http://%s/text_flask/?top_n=4\&data=%s"%(slave,string(xq)))
            slave_return_json = requests.get("http://10.102.35.45:5000/faiss_service/?nprobe=10&top_n=4&data=%s"%data_string)
            D_I_list_all_slave = json.loads(slave_return_json.text)['results']
            #D_I_list_all_slave = [[["6.6989465", "29088"], ["6.9235916", "8123"], ["6.9933767", "7843"], ["7.016519", "55215"]]]
            for list_each_dim in D_I_list_all_slave:
                for list in list_each_dim:
                    length_list = len(list)
                    if length_list == 2:
                        distance = float(list[0])
                        adid_index = list[1]
                        D_I_all_slave.append((adid_index,distance))
        end_query_slave_time = time.time()
        print("获取从节点信息，并且发送请求给从节点，并接收返回信息存储为list,所用时间：",end_query_slave_time-start_query_slave_time)

        #将上述二维数组中的数组合并为一个大数组,并将数据类型转换为float和int
        # start_connect_list = time.time()
        # D_I_all_slave = []
        # for list in D_I_list_all_slave:
        #     print(list)
        #     length_list = len(list)
        #     if length_list == 2:
        #         distance = float(list[0])
        #         adid_index = int(list[1])
        #         D_I_all_slave.append((adid_index,distance))
        # end_connect_list = time.time()
        #print("将二维数组合并为一个大数组，所用时间：",end_connect_list-start_connect_list)


        #重排序
        start_resort_time = time.time()
        result_all_sort_topk = sorted(D_I_all_slave,key=lambda d: d[1])[0:topk]
        end_resort_time = time.time()
        print("重排序成功，所用时间：",end_resort_time-start_resort_time)

        # 取出adid_index
        start_get_adid_index = time.time()
        result_evequery_adid_index = []
        for i in range(topk):
            result_evequery_adid_index.append(result_all_sort_topk[i][0])
        end_get_adid_index = time.time()
        print("返回adid_index:",end_get_adid_index-start_get_adid_index)

        # 查找adid的映射并返回
        start_get_adid = time.time()
        adid_return_evequery = []
        for adid_index in result_evequery_adid_index:
            adid = dict_adid_index[adid_index]
            adid_return_evequery.append(adid)
        end_get_adid = time.time()
        print("返回adid:" ,end_get_adid-start_get_adid)

        #返回结果，包括状态和adid列表
        start_data_return_to_user = time.time()
        result_data = {}
        result_data["status"] = "success"
        key = "results"
        result_data[key] = adid_return_evequery
        result_json = json.dumps(result_data)
        end_data_return_to_user = time.time()
        print("生成返回数据",end_data_return_to_user-start_data_return_to_user)

        end_all_service_time = time.time()
        print("************total time*********",end_all_service_time-start_all_service_time)
        return result_json
    except (AssertionError, ValueError):
        return "input error!!! 传入的维数不对"
    # except Exception:
    #     return "unknown error!!!"

@app.route("/exit_flask/")
def pku_seg2():
    os.system("kill -9 `ps aux|grep -v grep|grep uwsgiconfig.ini|awk '{print $2}'`")
    os._exit(0)


if __name__ == '__main__':
    path = hdfs.InsecureClient(url='http://10.21.52.65:50070;http://10.20.94.70:50070', user='hdfs')

    path_centreids_vector = "/region4/29297/app/develop/faiss_distribute/centroids_vector/part-00000"
    path_adid_index       = "/region4/29297/app/develop/faiss_distribute/adid_index/part-00000"
    ip_path = "/region4/29297/app/develop/faiss_distribute/ip_path.csv"

    start_get_centroid_and_adid_time = time.time()
    try:
        dict_centreids_vector = get_centreids_data(path,path_centreids_vector)
        dict_adid_index = get_adid_index_data(path,path_adid_index)
    except FileNotFoundError:
        print("the file of centre and adid isn't exit!!!!")
    end_get_centroid_and_adid_time = time.time()
    print("构建聚类中心和adid映射成功，所用时间：",end_get_centroid_and_adid_time-start_get_centroid_and_adid_time)

    start_slave_centreid_time = time.time()
    slave_path = "/region4/29297/app/develop/faiss_distribute/centroids_0/part-00000-c4b39d40-7270-4567-b6e3-48ea49e89b24-c000.csv"
    os.system("curl http://10.102.35.45:5000/start_update_index/?dim=32\&path=%s" % (slave_path))
    #dict_ip_path = get_ip_path(path, ip_path)
    #key：ip(string),value:path(string)
    # for key,value in dic_ip_path.items():
    #     os.system("curl http://%s:5000/start_update_index/?dim=128\&path=%s"%(key,value))
    end_slave_centreid_time = time.time()
    print("构建从节点与聚类中心的映射成功，所用时间：",end_slave_centreid_time-start_slave_centreid_time)

    app.run(host='0.0.0.0', debug=True)