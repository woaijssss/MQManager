#include <iostream>
#include "pro.h"
 
KafkaProducer::KafkaProducer(const string &brokers, const string &topics, int nPpartition /*= 1*/)
    : m_bRun(true), m_strTopics(topics), m_strBroker(brokers), m_nPpartition(nPpartition)
{
}
 
KafkaProducer::~KafkaProducer()
{
    Stop();
}
 
bool KafkaProducer::Init()
{
    string errstr = "";
 
    /*
     * Create configuration objects
     */
    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
 
    /*Set configuration properties,设置broker list*/
    if (conf->set("metadata.broker.list", m_strBroker, errstr) != RdKafka::Conf::CONF_OK){
        std::cerr << "RdKafka conf set brokerlist failed :" << errstr.c_str() << endl;
    }
    /* Set delivery report callback */
    conf->set("dr_cb", &m_producerDeliveryReportCallBack, errstr);
    conf->set("event_cb", &m_producerEventCallBack, errstr);
 
    /*
     * Create producer using accumulated global configuration.
    */
    m_pProducer = RdKafka::Producer::create(conf, errstr);
    if (!m_pProducer) {
        std::cerr << "Failed to create producer: " << errstr << std::endl;
        return false;
    }
    std::cout << "% Created producer " << m_pProducer->name() << std::endl;
    /*
     * Create topic handle.
    */
    m_pTopic = RdKafka::Topic::create(m_pProducer, m_strTopics,
                                      tconf, errstr);
    if (!m_pTopic) {
        std::cerr << "Failed to create topic: " << errstr << std::endl;
        return false;
    }
    return true;
}
void KafkaProducer::Send(const string &msg)
{
    if (!m_bRun)
        return;
    /*
     * Produce message
    */
    RdKafka::ErrorCode resp = m_pProducer->produce(m_pTopic, m_nPpartition,
                                                   RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
                                                   const_cast<char *>(msg.c_str()), msg.size(),
                                                   NULL, NULL);
    if (resp != RdKafka::ERR_NO_ERROR)
        std::cerr << "Produce failed: " << RdKafka::err2str(resp) << std::endl;
    else
        std::cerr << "Produced message (" << msg.size() << " bytes)" << std::endl;
 
    m_pProducer->poll(0);
 
    /* Wait for messages to be delivered */  //firecat add
    while (m_bRun && m_pProducer->outq_len() > 0) {
        std::cerr << "Waiting for " << m_pProducer->outq_len() << std::endl;
        m_pProducer->poll(1000);
    }
}
 
void KafkaProducer::Stop()
{
    delete m_pTopic;
    delete m_pProducer;
}


 
int main()
{
    //KafkaProducerClient* KafkaprClient_ = new KafkaProducerClient("localhost:9092", "test", 0);
 
    KafkaProducer* Kafkapr_ = new KafkaProducer("localhost:9092", "test", 0);
    Kafkapr_->Init();
    Kafkapr_->Send("hello world!");
 
    char str_msg[] = "Hello Kafka!";
 
    while (fgets(str_msg, sizeof(str_msg), stdin))
    {
        size_t len = strlen(str_msg);
        if (str_msg[len - 1] == '\n')
        {
            str_msg[--len] = '\0';
        }
 
        if (strcmp(str_msg, "end") == 0)
        {
            break;
        }
 
        Kafkapr_->Send(str_msg);
    }
 
    return 0;
}
