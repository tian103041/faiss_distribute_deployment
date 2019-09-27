#coding:utf-8
import time
import redis
import os

#功能：接收业务端发送过来的数据完成的信号，然后进行kmean聚类和pca降维并对每个类别分别存储不同的HDFS路径

#连接redis集群，并定义
redis_connection = redis.StrictRedis(host='10.193.22.154', port='11110', db=0)
ps = redis_connection.pubsub()

#实现redis的消息订阅
ps.subscribe('user_to_spark')


for item in ps.listen():        #监听状态：有消息发布了就拿过来
    if item['type'] == 'message':
        if bytes.decode(item['data']) == 'the model data is ready!':

            print("ready to kmean cluster and save data to hdfs")
            os.system('nohup spark2-submit --queue root.ai.algorithm --num-executors 50 --executor-memory 4G --conf spark.dynamicAllocation.enabled=false --files /home/11085098/item_emb_w --jars /home/11085098/faiss/faiss_distribute/spark/jedis-2.8.1.jar,/home/11085098/faiss/faiss_distribute/spark/commons-pool2-2.4.2.jar --class cluster.kmeans_cluster xyz.vivo.ai-1.0-SNAPSHOT.jar > /home/11085098/faiss/faiss_distribute/log/$(date +\%Y-\%m-\%d-\%H:\%M:\%S).log &')