#!/usr/bin/env python
# coding: utf-8

# In[1]:


import sys
import json
import pandas as pd
import os
from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.errors import KafkaError
import datetime
 
KAFAKA_HOST = "127.0.0.1"  #服务器端口地址
KAFAKA_PORT = 9092             #端口号
KAFAKA_TOPIC = "test"        #topic
 
data=pd.read_csv('F:\\NS3-data\\test\sim0_cl0_bufferUnderrunLog.csv')
key_value=data.to_json()

class Kafka_consumer():
 
 
    def __init__(self, kafkahost, kafkaport, kafkatopic, groupid,key):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.groupid = groupid
        self.key = key
        self.consumer = KafkaConsumer(self.kafkatopic, group_id=self.groupid,
                                      bootstrap_servers='{kafka_host}:{kafka_port}'.format(
                                          kafka_host=self.kafkaHost,
                                          kafka_port=self.kafkaPort)
                                      )
 
    def consume_data(self):
        try:
            for message in self.consumer:
                yield message
        except KeyboardInterrupt as e:
            print(e)

def sortedDictValues(adict):
    items = adict.items()
    items=sorted(items,reverse=False)
    return [value for key, value in items]
 
def main(xtype, group, key):
    '''
    测试consumer和producer
    '''
    if xtype == 'c':
        # 消费模块
        consumer = Kafka_consumer(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_TOPIC, group,key)
        print("===========> consumer:", consumer)
        count = 0
        message = consumer.consume_data()
        for msg in message:
            end = datetime.datetime.now()
            count = count + 1
            print(count, end)
            msg=msg.value.decode('utf-8')
            #python_data=json.loads(msg)   ##这是一个字典
            python_data=msg  ##这是一个字典
            #print(msg)
            #key_list=list(python_data)
            #test_data=pd.DataFrame()
            #for index in key_list:
            #    print(index)
            #    if index=='Month':
            #        a1=python_data[index]
            #        data1 = sortedDictValues(a1)
            #        test_data[index]=data1
            #    else:
            #        a2 = python_data[index]
            #        data2 = sortedDictValues(a2)
            #        test_data[index] = data2
            #        print(test_data)
 
 
 
 
if __name__ == '__main__':
    main(xtype='c',group='py_test',key=None)


# In[ ]:





# In[ ]:




