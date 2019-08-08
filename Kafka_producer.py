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
 
KAFAKA_HOST = "127.0.0.1"  #服务器端口地址
KAFAKA_PORT = 9092             #端口号
KAFAKA_TOPIC = "test"        #topic
 
data=pd.read_csv('F:\\NS3-data\\test\sim0_cl0_bufferUnderrunLog.csv')
key_value='count = aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'#data.to_json()
class Kafka_producer():
    '''
    生产模块：根据不同的key，区分消息
    '''
 
    def __init__(self, kafkahost, kafkaport, kafkatopic, key):
        self.kafkaHost = kafkahost
        self.kafkaPort = kafkaport
        self.kafkatopic = kafkatopic
        self.key = key
        self.producer = KafkaProducer(bootstrap_servers='{kafka_host}:{kafka_port}'.format(
            kafka_host=self.kafkaHost,
            kafka_port=self.kafkaPort,
            max_in_flight_requests_per_connection=1)
        )
 
    def sendjsondata(self, params):
        try:
            parmas_message = params      #注意dumps
            producer = self.producer
            producer.send(self.kafkatopic, key=self.key, value=parmas_message.encode('utf-8'))
            producer.flush()
        except KafkaError as e:
            print(e)

def sortedDictValues(adict):
    items = adict.items()
    items=sorted(items,reverse=False)
    return [value for key, value in items]
 
def main(xtype, group, key):
    '''
    测试consumer和producer
    '''
    if xtype == "p":
        # 生产模块
        producer = Kafka_producer(KAFAKA_HOST, KAFAKA_PORT, KAFAKA_TOPIC, key)
        print("===========> producer:", producer)
        params =key_value
        for i in range(5000):
            producer.sendjsondata(params)
 
 
if __name__ == '__main__':
    main(xtype='p',group='py_test',key=None)


# In[ ]:





# In[ ]:





# In[ ]:




