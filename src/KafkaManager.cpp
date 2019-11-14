/*
 * KafkaManager.cpp
 *
 *  Created on: Nov 14, 2019
 *      Author: wenhan
 */

#include <iostream>

#include "KafkaManager.h"

using namespace std;

KafkaManager::KafkaManager(const std::string& brokers,
                           const std::string& topics,
                           const std::string& groupId,
                           int chr,
                           long offset,
                           int nPpartition)
        : m_chr(chr),
          m_brokers(brokers),
          m_topics(topics),
          m_groupId(groupId),
          m_nPpartition(nPpartition)
{

}

//KafkaManager::KafkaManager(const std::string& brokers,
//                           const std::string& topics,
//                           const std::string& groupid,
//                           int chr,
//                           int nPpartition)
//        : m_chr(chr)
//{
//
//}

KafkaManager::~KafkaManager()
{
        if (m_chr == CONSUMER)
        {
                this->freeConsumer();
        } else
        {
                this->freeProducer();
        }
}
void KafkaManager::freeConsumer()
{
        m_pKafkaConsumer->stop(m_pTopic, m_partition);
        if (m_pTopic)
        {
                delete m_pTopic;
                m_pTopic = nullptr;
        }
        if (m_pKafkaConsumer)
        {
                delete m_pKafkaConsumer;
                m_pKafkaConsumer = nullptr;
        }

        /*销毁kafka实例*/
        RdKafka::wait_destroyed(5000);
}

void KafkaManager::freeProducer()
{

}

/* 初始化 MQ 参数 */
void KafkaManager::init()
{
        if (m_chr == CONSUMER)
        {
                this->initConsumer();
        } else
        {
                this->initProducer();
        }
}

/* 启动 MQ 连接 */
void KafkaManager::start()
{

}

void KafkaManager::initProducer()
{
        string errstr = "";

        /*
         * Create configuration objects
         */
        RdKafka::Conf* conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        RdKafka::Conf* tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

        /*Set configuration properties,设置broker list*/
        if (conf->set("metadata.broker.list", m_brokers, errstr) != RdKafka::Conf::CONF_OK)
        {
                std::cerr << "RdKafka conf set brokerlist failed :" << errstr.c_str() << endl;
        }
        /* Set delivery report callback */
        conf->set("dr_cb", (DeliveryReportCb*)this, errstr);
        conf->set("event_cb", (EventCb*)this, errstr);

        /*
         * Create producer using accumulated global configuration.
         */
        m_pProducer = RdKafka::Producer::create(conf, errstr);
        if (!m_pProducer)
        {
                std::cerr << "Failed to create producer: " << errstr << std::endl;
                return;
        }
        std::cout << "% Created producer " << m_pProducer->name() << std::endl;
        /*
         * Create topic handle.
         */
        m_pTopic = RdKafka::Topic::create(m_pProducer, m_topics, tconf, errstr);
        if (!m_pTopic)
        {
                std::cerr << "Failed to create topic: " << errstr << std::endl;
                return;
        }
}

/* 向 MQ 生产一条消息 */
void KafkaManager::product(const string& msg)
{
        /*
         * Produce message
         */
        RdKafka::ErrorCode resp = m_pProducer->produce(m_pTopic, m_nPpartition,
                                                       RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                                       const_cast<char*>(msg.c_str()), msg.size(),
                                                       NULL,
                                                       NULL);
        if (resp != RdKafka::ERR_NO_ERROR)
                std::cerr << "Produce failed: " << RdKafka::err2str(resp) << std::endl;
        else
                std::cerr << "Produced message (" << msg.size() << " bytes)" << std::endl;

        m_pProducer->poll(0);

        /* Wait for messages to be delivered */  //firecat add
        while (m_pProducer->outq_len() > 0)
        {
                std::cerr << "Waiting for " << m_pProducer->outq_len() << std::endl;
                m_pProducer->poll(1000);
        }
}

void KafkaManager::initConsumer()
{
        RdKafka::Conf* conf = nullptr;
        conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
        if (!conf)
        {
                cout << "RdKafka create global conf failed" << endl;
                return;
        }

        std::string errstr;
        /*设置broker list*/
        if (conf->set("bootstrap.servers", m_brokers, errstr) != RdKafka::Conf::CONF_OK)
        {
                cout << "RdKafka conf set brokerlist failed: " << errstr << endl;
        }

        /*设置consumer group*/
        if (conf->set("group.id", m_groupId, errstr) != RdKafka::Conf::CONF_OK)
        {
                cout << "RdKafka conf set group.id failed: " << errstr << endl;
        }

        std::string strfetch_num = "10240000";
        /*每次从单个分区中拉取消息的最大尺寸*/
        if (conf->set("max.partition.fetch.bytes", strfetch_num, errstr) != RdKafka::Conf::CONF_OK)
        {
                cout << "RdKafka conf set max.partition failed: " << errstr << endl;
        }

        /*创建kafka consumer实例*/
        m_pKafkaConsumer = RdKafka::Consumer::create(conf, errstr);
        if (!m_pKafkaConsumer)
        {
                cout << "failed to ceate consumer" << endl;
        }
        delete conf;

        RdKafka::Conf* tconf = nullptr;
        /*创建kafka topic的配置*/
        tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
        if (!tconf)
        {
                cout << "RdKafka create topic conf failed" << endl;
                return;
        }

        /*kafka + zookeeper,当消息被消费时,会想zk提交当前groupId的consumer消费的offset信息,
         当consumer再次启动将会从此offset开始继续消费.在consumter端配置文件中(或者是
         ConsumerConfig类参数)有个"autooffset.reset"(在kafka 0.8版本中为auto.offset.reset),
         有2个合法的值"largest"/"smallest",默认为"largest",此配置参数表示当此groupId下的消费者,
         在ZK中没有offset值时(比如新的groupId,或者是zk数据被清空),consumer应该从哪个offset开始
         消费.largest表示接受接收最大的offset(即最新消息),smallest表示最小offset,即从topic的
         开始位置消费所有消息.*/
        if (tconf->set("auto.offset.reset", "smallest", errstr) != RdKafka::Conf::CONF_OK)
        {
                cout << "RdKafka conf set auto.offset.reset failed: " << errstr << endl;
        }

        m_pTopic = RdKafka::Topic::create(m_pKafkaConsumer, m_topics, tconf, errstr);
        if (!m_pTopic)
        {
                cout << "RdKafka create topic failed: " << errstr << endl;
        }
        delete tconf;

        RdKafka::ErrorCode resp = m_pKafkaConsumer->start(m_pTopic, m_partition, m_offset);
        if (resp != RdKafka::ERR_NO_ERROR)
        {
                cout << "failed to start consumer: " << RdKafka::err2str(resp) << endl;
        }

}

/* 从 MQ 消费一条消息 */
std::string KafkaManager::consume(const int& timeoutMs)
{
        string msg;
        RdKafka::Message* message = m_pKafkaConsumer->consume(m_pTopic, m_partition, timeoutMs);
        msg = this->parseMessage(message);
        m_pKafkaConsumer->poll(0);
        delete message;

        return msg;
}

std::string KafkaManager::parseMessage(RdKafka::Message* message)
{
        string msg;

        switch (message->err())
        {
        case RdKafka::ERR__TIMED_OUT:
                break;
        case RdKafka::ERR_NO_ERROR:
                msg = string(static_cast<const char*>(message->payload()));
                m_lastOffset = message->offset();

                break;
        case RdKafka::ERR__PARTITION_EOF:
                std::cerr << "%% Reached the end of the queue, offset: " << m_lastOffset << std::endl;
                break;
        case RdKafka::ERR__UNKNOWN_TOPIC:
        case RdKafka::ERR__UNKNOWN_PARTITION:
                std::cerr << "Consume failed: " << message->errstr() << std::endl;
                break;
        default:
                std::cerr << "Consume failed: " << message->errstr() << std::endl;
                break;
        }

        return msg;
}

void KafkaManager::dr_cb(RdKafka::Message& message)
{
        std::cout << "Message delivery for (" << message.len() << " bytes): " << message.errstr() << std::endl;
        if (message.key())
                std::cout << "Key: " << *(message.key()) << ";" << std::endl;
}

void KafkaManager::event_cb(RdKafka::Event& event)
{
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
                std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str() << std::endl;
                if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
                {
                        break;
                }
        case RdKafka::Event::EVENT_STATS:
                std::cerr << "\"STATS\": " << event.str() << std::endl;
                break;
        case RdKafka::Event::EVENT_LOG:
                break;
        default:
                break;
        }
}
