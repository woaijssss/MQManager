/*
 * main.cpp
 *
 *  Created on: Nov 14, 2019
 *      Author: wenhan
 */

#include <iostream>
#include "KafkaManager.h"

using namespace std;

int main(int argc, char** argv)
{
#ifdef KCONSUMER
        KafkaManager km("localhost:9092", "test", "0");
        km.init();

        while (true)
        {
                string msg = km.consume(100);
                cout << msg << endl;
        }
#else
        KafkaManager km("localhost:9092", "test", "0", KafkaManager::PRODUCER);
        km.init();

        int i = 0;
        while (true)
        {
                string msg = to_string(i++);
                cout << msg << endl;
                km.product(msg);
        }
#endif

        return 0;
}
