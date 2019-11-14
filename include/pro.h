#ifndef KAFKAPRODUCER_H
#define KAFKAPRODUCER_H
 
#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <list>
#include <librdkafka/rdkafkacpp.h>
#include <vector>
#include <fstream>
 
using std::string;
using std::list;
using std::cout;
using std::endl;
using std::vector;
using std::fstream;
 
class KafkaProducerDeliveryReportCallBack : public RdKafka::DeliveryReportCb {
public:
    void dr_cb(RdKafka::Message &message) {
        std::cout << "Message delivery for (" << message.len() << " bytes): " <<
            message.errstr() << std::endl;
        if (message.key())
            std::cout << "Key: " << *(message.key()) << ";" << std::endl;
    }
};
class KafkaProducerEventCallBack : public RdKafka::EventCb {
public:
    void event_cb(RdKafka::Event &event) {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                event.str() << std::endl;
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
            break;
        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;
        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n",
                event.severity(), event.fac().c_str(), event.str().c_str());
            break;
        default:
            std::cerr << "EVENT " << event.type() <<
                " (" << RdKafka::err2str(event.err()) << "): " <<
                event.str() << std::endl;
            break;
        }
    }
};
class KafkaProducer
{
public:
    KafkaProducer(const string &brokers, const string &topics, int nPpartition = 0);
    virtual ~KafkaProducer();
    bool Init();
    void Send(const string &msg);
    void Stop();
private:
    RdKafka::Producer *m_pProducer = NULL;
    RdKafka::Topic *m_pTopic = NULL;
    KafkaProducerDeliveryReportCallBack m_producerDeliveryReportCallBack;
    KafkaProducerEventCallBack m_producerEventCallBack;
    std::string m_strTopics;
    std::string m_strBroker;
    bool m_bRun = false;
    int m_nPpartition = 0;
};
#endif // KAFKAPRODUCER_H
