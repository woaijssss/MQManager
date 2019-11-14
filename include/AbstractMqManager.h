/*
 * AbstractMqManager.h
 *
 *  Created on: Nov 14, 2019
 *      Author: wenhan
 */

#ifndef __INCLUDE_ABSTRACTMQMANAGER_H__
#define __INCLUDE_ABSTRACTMQMANAGER_H__

#include <string>

class AbstractMqManager
{
public:
        virtual ~AbstractMqManager()
        {

        }

public: // 基本四大接口必须要实现
        /* 初始化 MQ 参数 */
        virtual void init() = 0;

        /* 启动 MQ 连接 */
        virtual void start() = 0;

        /* 向 MQ 生产一条消息 */
        virtual void product(const std::string& msg) = 0;

        /* 从 MQ 消费一条消息 */
        virtual std::string consume(const int& timeoutMs) = 0;
};

#endif /* __INCLUDE_ABSTRACTMQMANAGER_H__ */
