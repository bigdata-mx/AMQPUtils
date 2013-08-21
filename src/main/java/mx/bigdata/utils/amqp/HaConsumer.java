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

import java.util.Map;
import java.util.HashMap;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import mx.bigdata.utils.amqp.AMQPClientHelper;

public abstract class HaConsumer extends ReconnectingConsumer {  

  private final static int DEFAULT_HEARTBEAT_SECONDS = 50;
  
  public HaConsumer(String tag, String key, AMQPClientHelper amqp, 
		    ConnectionFactory factory) {
    super(tag, key, amqp, factory);
    if (factory.getRequestedHeartbeat() == 0) {
      factory.setRequestedHeartbeat(DEFAULT_HEARTBEAT_SECONDS);
    }
  }

  public HaConsumer(String tag, String key, AMQPClientHelper amqp) 
    throws Exception {
    this(tag, key, amqp, amqp.createConnectionFactory(key));
  }

  protected String createQueue(Channel channel, String key) throws Exception {
    return amqp.createQueue(channel, key, true, false, false);
  }
}
