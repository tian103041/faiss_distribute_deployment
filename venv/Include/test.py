
import numpy as np
import json
import os

# d1 = {'a': 1, 'b': 4, 'c': 2, 'f' : 12}
# d2 = {'m' : 56, 'n' : 89}
# d = {}
#
# d.update(d1)
# d.update(d2)
#
#
# print(d)

# slave = ["10.102.23.107","10.102.23.108","10.102.23.109"]
# numClusters = 3
# centreid_slave = [0 for i in range(numClusters)]
# for i in range(0,numClusters):
#     centreid_slave[i] = slave[i]
#
# print(centreid_slave)

# a = [1,2,3,4,5,6]
# vector1 = np.array(a)
# b = [4,5,6,7,8,9]
# vector2 = np.array(b)
#
# c = np.linalg.norm(vector1 - vector2)
# print(c)


# slave_result = { "state":"sucess", "result":[[1,2,3],[4,5,6],[7,8,9]]}
# print(type(slave_result))
# print(slave_result)
# result_json = json.dumps(slave_result)
# print(result_json)
# print(type(result_json))
#
#
# slave_return = json.loads(result_json)['result']
# print(len(slave_return))
# slave_result_list = slave_return["result"]
#
# print(slave_result_list)


data_lsit=[(18,23),(13,14),(5,65),(7,18)]
data_sort = sorted(data_lsit,key=lambda d: d[1])[0:3]
for i in range(3):
    print(data_sort[i])
print(data_sort)

# result_list = [
#     [[(1,2),(3,1)],[(5,6),(7,8)]],
#     [[(9,56),(11,12)],[(13,23),(33,13)]],
#     [[(16,58),(39,43)],[(17,29),(35,44)]]]
#
#
# result_return_all = []
# for j in range(0,len(data_lsit)):
#
#     result = []
#     for i in range(0,len(result_list)):
#         result.append(result_list[i][j])
#
#     result_all = []
#     for m in range(len(result)):
#         for n in range(len(result[0])):
#             result_all.append(result[m][n])
#
#     result_sort = sorted(result_all)[0:3]
#     print(result_sort)
#     result_return = []
#     for i in range(len(result_sort)):
#         result_return.append(result_sort[i][1])
#
#     result_return_all.append(result_return)
#
# print(result_return_all)

# for j in range(0, len_every_query):
#
#     #取出每台slave对同一条向量的查询结果
#     same_query_dif_slave = []
#     for i in range(0, len_return_slave):
#         same_query_dif_slave.append(dif_query_and_slave[i][j])
#     print("每条向量的查询结果")
#
#     #拼接成一个list
#     same_query_connect = []
#     for m in range(len_return_slave):
#         for n in range(topk):
#             same_query_connect.append(same_query_dif_slave[m][n])
#     print("每条向量的查询结果拼接成功")
#
#     #排序以及得到topk
#     result_all_sort_topk = sorted(result_all)[0:topk]
#     print("每条向量的查询结果拼接，并排序成功")
#
#     #取出adid_index
#     result_evequery_adid_index = []
#     for i in range(topk):
#         result_evequery_adid_index.append(result_all_sort_topk[i][1])
#     print("返回adid_index")
#
#     # 查找adid的映射并返回
#     adid_return_evequery = []
#     for adid_index in result_evequery_adid_index:
#         adid = dict_adid_index[adid_index]
#         adid_return_evequery.append(adid)
#     print("返回adid")
#
#     result_all_return.append(adid_return_evequery)


# path = hdfs.InsecureClient(url='http://10.21.52.65:50070;http://10.20.94.70:50070', user='hdfs')
# path_centreids_vector = "/region4/29297/app/develop/faiss_distribute/ip_path.csv"
#
# dict_ip_path = {}
# #可以把字典存储在外面，整理一个对应的文件，读取相应文件，
#
# for key,value in dict_ip_path.items():
#     print(key)
#     print(value)
#
# with path.read(path_centreids_vector) as csvfile:
#     csv_reader = csvfile.read()
#     byte_decode = bytes.decode(csv_reader)
#     line_split = byte_decode.split("\r\n")
#     for i in line_split:
#         if i==" ":
#             line_split.remove(i)
#     for ip_path in line_split:
#         ip_path_split = ip_path.split("\t")
#         if len(ip_path_split)==2:
#             dict_ip_path[ip_path_split[0]] = ip_path_split[1]
#
# for key, value in dict_ip_path.items():
#     print("curl http://%s:5000/start_update_index/?dim=128\&path=%s" % (key, value))
#
#
#
#     for row in csv_reader:
#         print(row)
#
#
#
#         birth_data.append(row)

# with path.read(path_centreids_vector) as reader:
#     data_hdfs = reader.read()
#     data_str_all = bytes.decode(data_hdfs)
#     line_split_all = data_str_all.split("\n")
#     len_split = len(line_split_all)
#     line_split_every_data = line_split_all[0:len_split-1]
#     for line in line_split_every_data:
#         line_replace = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "").replace("\n","")
#         line_split = line_replace.split(",")
#         centreid = int(line_split[0])
#         centreid_vector = []
#         for i in range(1, len(line_split)):
#             centreid_vector.append(float(line_split[i]))
#         dict_centreids_vector[centreid] = centreid_vector
#
# dict_adid_index = {}
# path_adid_index = "/region4/29297/app/develop/faiss_distribute/adid_index/part-00000"
# with path.read(path_adid_index) as reader:
#     data_hdfs = reader.read()
#     data_str_all = bytes.decode(data_hdfs)
#     line_split_all = data_str_all.split("\n")
#     for i in line_split_all:
#         if i == '':
#             line_split_all.remove(i)
#     for line in line_split_all:
#         line_split = line.replace("(", "").replace(")", "").replace("g", "").replace("_", "").split(",")
#         adid = line_split[0]
#         adid_index = line_split[1]
#         dict_adid_index[adid_index] = adid
#
# for key in dict_adid_index:
#     print(key)
#
# print(len(dict_adid_index))
#
#
#     for line in line_split_all:
#         line_split = line.replace("(","").replace(")","").replace("g","").replace("_","").split(",")
#         adid = line_split[0]
#         adid_index = line_split[1]
#         dict_adid_index[adid_index] = adid
#
#
#
#
#
# for values in dict_centreids_vector.values():
#     print(len(values))
#     print(type(values))
#
#
# len(dict_centreids_vector)
#
# files_centreids_vector = os.listdir(path_centreids_vector)
# for file_centreids_vector in files_centreids_vector:  # 遍历文件夹
#        f_centreids_vector = path.open(path_centreids_vector + "/" + file_centreids_vector)  # 打开文件
#        for line in f_centreids_vector.readlines():
#             line_replace = line.replace("(", "").replace(")", "").replace("[", "").replace("]", "")
#             line_replace_ = line_replace.replace("\n", "").split(",")
#             centreid = int(line_replace_[0])
#             centreid_vector = []
#             for i in range(1, len(line_replace_)):
#                 centreid_vector.append(float(line_replace_[i]))
#             dict_centreids_vector[centreid] = centreid_vector