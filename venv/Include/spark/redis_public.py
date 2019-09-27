#coding:utf-8
import redis
import  time


rc = redis.StrictRedis(host='10.193.22.154', port='11110', db=0)

value = 'hdfs data is ready!'
rc.publish('message_from_spark', value)