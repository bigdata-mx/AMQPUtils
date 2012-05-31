/*
 *  Copyright 2010 BigData Mx
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package mx.bigdata.utils.amqp;

import java.util.List;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

public interface AMQPClientHelper {  
  ConnectionFactory createConnectionFactory() throws Exception;  

  ConnectionFactory createConnectionFactory(String key) throws Exception;  

  Channel declareChannel(ConnectionFactory factory, String key) 
    throws Exception;

  Channel declareChannel(ConnectionFactory factory, String key, 
			 boolean delcareExchange) 
    throws Exception;

  String createQueue(Channel channel, String key) throws Exception;  

  @Deprecated
  String createQueue(Channel channel, String key, boolean nonExclusive) 
    throws Exception;  

  String createQueue(Channel channel, String key, boolean durable, 
		     boolean exclusive, boolean autoDelete) throws Exception;  

  String createQueue(Channel channel, String key, boolean durable, 
		     boolean exclusive, boolean autoDelete, 
		     Map<String, Object> args) throws Exception;
  
  String createNamedQueue(Channel channel, String key) 
    throws Exception;  
  
  String getRoutingKey();  

  String getRoutingKey(String key);

  String getExchangeName(String key);

  String getExchangeType(String key);

  String getHaPolicy(String key);

  List<String> getHaPolicyParams(String key);

  QueueingConsumer createQueueingConsumer(Channel channel, String queue) 
    throws Exception;

  byte[] getBodyAndAck(Channel channel, QueueingConsumer consumer) 
    throws Exception;

  void ack(Channel channel, QueueingConsumer.Delivery delivery) 
    throws Exception;

  void reject(Channel channel, QueueingConsumer.Delivery delivery) 
    throws Exception;
}