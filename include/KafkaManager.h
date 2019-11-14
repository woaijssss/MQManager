/*
 * KafkaManager.h
 *
 *  Created on: Nov 14, 2019
 *      Author: wenhan
 */

#ifndef __INCLUDE_KAFKAMANAGER_H__
#define __INCLUDE_KAFKAMANAGER_H__

#include "librdkafka/rdkafkacpp.h"

#include "AbstractMqManager.h"

class KafkaManager: public AbstractMqManager,
                    public RdKafka::DeliveryReportCb,
                    public RdKafka::EventCb
{
public:
        KafkaManager(const std::string& brokers, const std::string& topics, const std::string& groupid, int chr =
                             CONSUMER,
                     long offset = -1, int nPpartition = 0);
        virtual ~KafkaManager();

public:
        /* 初始化 MQ 参数 */
        virtual void init();

        /* 启动 MQ 连接 */
        virtual void start();

        /* 向 MQ 生产一条消息 */
        virtual void product(const std::string& msg);

        /* 从 MQ 消费一条消息 */
        virtual std::string consume(const int& timeoutMs);

public:
        void initConsumer();
        void initProducer();

        void freeConsumer();
        void freeProducer();

public:
        /* 使用者角色 */
        enum Character
        {
                CONSUMER = 0,
                PRODUCER
        };

private:// consumer 调用
        std::string parseMessage(RdKafka::Message* message);

private:
        void dr_cb(RdKafka::Message& message);
        void event_cb(RdKafka::Event& event);

private:
        int m_chr;      // 角色
private:// consumer 参数
        std::string m_brokers;      // ip:port
        std::string m_topics;    // 消息队列主题
        std::string m_groupId;  // 组id

        long m_lastOffset = 0;

        RdKafka::Consumer* m_pKafkaConsumer = nullptr;
        RdKafka::Topic* m_pTopic = nullptr;
        long m_offset = RdKafka::Topic::OFFSET_BEGINNING;
        int m_partition = 0;

private:// producer 参数
        int m_nPpartition;
        RdKafka::Producer* m_pProducer = nullptr;
};

#endif /* __INCLUDE_KAFKAMANAGER_H__ */
