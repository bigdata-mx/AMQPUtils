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

  public HaConsumer(String tag, String key, AMQPClientHelper amqp, 
		    ConnectionFactory factory) {
    super(tag, key, amqp, factory);
  }

  public HaConsumer(String tag, String key, AMQPClientHelper amqp) 
    throws Exception {
    super(tag, key, amqp);
  }

  protected String createQueue(Channel channel, String key) throws Exception {
     Map<String, Object> args = new HashMap<String, Object>();
     String policy = amqp.getHaPolicy(key);
     if (policy != null) {
       args.put("x-ha-policy", amqp.getHaPolicy(key));
       if (policy.equals("nodes")) {
	 args.put("x-ha-policy-params", amqp.getHaPolicyParams(key));
       }
     }
     return amqp.createQueue(channel, key, true, false, false, args);
  }
}