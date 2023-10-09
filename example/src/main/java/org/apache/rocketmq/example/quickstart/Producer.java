/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * This class demonstrates how to send messages to brokers using provided {@link DefaultMQProducer}.
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {

        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer("please_rename_unique_group_name_1");

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */

        producer.setNamesrvAddr("127.0.0.1:9876");

        /*
         * Launch the instance.
         */
        producer.start();


        for (int i = 0; i < 1; i++) {
            try {

                /*
                 * Create a message instance, specifying topic, tag and message body.
                 */
//                Message msg = new Message("topic-lan-test-20230608-17" /* Topic *//* Tag */,
//                    ("Hello RocketMQ " + i + "1").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//                );
                Message msg1 = new Message("topic-lan-test-20230608-17" /* Topic *//* Tag */,
                        ("[{\"vendorId\":7,\"reqId\":\"7-419-167194-PO062306114190006-D897-4-1686636545000\",\"uuid\":\"459f0bf8-4fe7-4c95-9287-f4812d8f457a\",\"storeId\":168,\"sapId\":\"419\",\"storeName\":\"Lucky Express Kamboul\",\"skuId\":101849264,\"matnr\":\"167194\",\"wareName\":\"CHEA NGHOUN BEEF MEAT BALL-B 300G FT\",\"stockStore\":9999,\"stockStoreName\":\"计划库存地\",\"stockUnit\":\"EA\",\"businessTypeCode\":\"D897\",\"businessTypeDec\":\"采购单关单\",\"num\":2.00000,\"unit\":\"EA\",\"businessVoucherCode\":\"PO062306114190006\",\"creditRowCode\":\"4\",\"accountTime\":\"2023-06-13T05:09:05.000+00:00\",\"accountTimeUtc\":1686636545000,\"wareInfo\":\"{\\\"specType\\\":0,\\\"isMgtStock\\\":true,\\\"sellType\\\":1}\",\"stockInfo\":\"{\\\"dio\\\":{\\\"stock\\\":{\\\"01\\\":3},\\\"stockTime\\\":{\\\"01\\\":\\\"2023-06-13 14:09:04\\\"},\\\"occupyStock\\\":{\\\"01\\\":0},\\\"occupyStockTime\\\":{\\\"01\\\":\\\"2023-06-12 10:28:12\\\"},\\\"waitForConfirmed\\\":0.0000,\\\"waitForConfirmedTime\\\":\\\"2023-05-18 14:11:11\\\",\\\"waitForReceive\\\":2.0000,\\\"waitForReceiveTime\\\":\\\"2023-06-13 12:01:15\\\",\\\"onPassage\\\":0.0000,\\\"onPassageTime\\\":\\\"2023-06-13 14:09:05\\\",\\\"version\\\":1503835,\\\"tag\\\":0},\\\"dic\\\":{\\\"stock\\\":{\\\"01\\\":3},\\\"stockTime\\\":{\\\"01\\\":\\\"2023-06-13 14:09:04\\\"},\\\"occupyStock\\\":{\\\"01\\\":0},\\\"occupyStockTime\\\":{\\\"01\\\":\\\"2023-06-12 10:28:12\\\"},\\\"waitForConfirmed\\\":0.0000,\\\"waitForConfirmedTime\\\":\\\"2023-05-18 14:11:11\\\",\\\"waitForReceive\\\":2.0000,\\\"waitForReceiveTime\\\":\\\"2023-06-13 12:01:15\\\",\\\"onPassage\\\":0.0000,\\\"onPassageTime\\\":\\\"2023-06-13 14:09:05\\\",\\\"version\\\":1503836,\\\"tag\\\":0}}\",\"inventoryCode\":\"9999\",\"variationOccupy\":0,\"source\":\"STORE\"}]").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
                );
//                Message msg2 = new Message("topic-lan-test-20230608-14" /* Topic *//* Tag */,
//                        ("Hello RocketMQ " + i+"2").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//                );
//                Message msg3 =  new Message("topic-lan-test-20230608-14" /* Topic *//* Tag */,
//                        ("Hell o RocketMQ " + i+"3").getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//                );
//                Message msg1 = new Message("TopicTest1" /* Topic */,
//                        "TagA" /* Tag */,
//                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
//                );
//                List<Message> list = new ArrayList<>();
//                list.add(msg);
//                list.add(msg2);
//                list.add(msg3);
                /*
                 * Call send message to deliver message to one of brokers.
                 */
//                SendResult sendResult = producer.send(list);
                SendResult sendResult1 = producer.send(msg1);

                System.out.printf("%s%n", sendResult1);
            } catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        /*
         * Shut down once the producer instance is not longer in use.
         */
        producer.shutdown();
    }
}
