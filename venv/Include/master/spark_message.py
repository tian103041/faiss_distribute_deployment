import redis
import os

#功能：接收业务端发送过来的数据完成的信号，然后进行kmean聚类和pca降维并对每个类别分别存储不同的HDFS路径

#连接redis集群，并定义
redis_connection = redis.StrictRedis(host='10.193.22.154', port='11110', db=0)
ps = redis_connection.pubsub()

#实现redis的消息订阅
ps.subscribe('message_from_spark')

for item in ps.listen():        #监听状态：有消息发布了就拿过来
    if item['type'] == 'message':
        if bytes.decode(item['data']) == 'hdfs data is ready!' :
            print("hdfs data is ready!")
            os.system("nohup python master_service.py > faiss_distribute_service.log &")