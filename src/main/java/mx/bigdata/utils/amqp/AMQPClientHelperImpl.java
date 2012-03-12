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

//import java.util.concurrent.ExecutorService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Address;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import mx.bigdata.anyobject.AnyObject;

public final class AMQPClientHelperImpl implements AMQPClientHelper {
  
  private static final int DEFAULT_QOS = 1000;

  private static final String DEFAULT_ROUTING_KEY = "-";

  private final AnyObject conf;

  public AMQPClientHelperImpl(AnyObject conf) {
    this.conf = conf;
  }

  public ConnectionFactory createConnectionFactory() throws Exception {
    return createConnectionFactory(null);
  }

  public ConnectionFactory createConnectionFactory(String key) 
    throws Exception {
    ConnectionFactory factory = new ConnectionFactory();
    String username = getValueOrDefault("amqp_username", key);
    if (username != null) {
      factory.setUsername(username);
    }
    String password = getValueOrDefault("amqp_password", key);
    if (password != null) {
      factory.setPassword(password);
    }
    String virtualHost = getValueOrDefault("amqp_virtual_host", key);
    if ( virtualHost != null) {
      factory.setVirtualHost(virtualHost);
    }
    String host = getValueOrDefault("amqp_host", key);
    if (host != null) {
      factory.setHost(host);
    }
    Integer port = getIntegerOrDefault("amqp_port", key);
    if (port != null) {
      factory.setPort(port);
    }
    Boolean notifyCancel = 
      getBooleanOrDefault("amqp_consumer_cancel_notify", key);
    if (notifyCancel != null && notifyCancel) {
      Map<String, Object> properties = new HashMap<String, Object>();
      Map<String, Object> capabilities = new HashMap<String, Object>();
      capabilities.put("consumer_cancel_notify", true);
      properties.put("capabilities", capabilities);
      factory.setClientProperties(properties);
    }
    return factory;
  }

  public Channel declareChannel(ConnectionFactory factory, String key) 
    throws Exception {
    Address[] hosts = getMultipleHosts();
    Connection conn;
    if (hosts == null) {
      conn = factory.newConnection();
    } else {
      conn = factory.newConnection(hosts);
    }
    Channel channel = conn.createChannel();
    Integer basicQos = conf.getInteger("channel_basic_qos");
    if (basicQos != null) {
      channel.basicQos(basicQos);
    } else {
      channel.basicQos(DEFAULT_QOS);
    }
    channel.exchangeDeclare(getExchangeName(key), 
                            getExchangeType(key), 
                            true);
    return channel;
  }

  public String createQueue(Channel channel, String key) throws Exception {
    AMQP.Queue.DeclareOk result = channel.queueDeclare();
    String queue = result.getQueue();
    channel.queueBind(queue, getExchangeName(key), getRoutingKey(key));
    return queue;
  }

  @Deprecated
  public String createQueue(Channel channel, String key, boolean nonExclusive)
    throws Exception {
    return createNamedQueue(channel, key);
  }

  public String createNamedQueue(Channel channel, String key)
    throws Exception {
    return createQueue(channel, key, false, false, true);
  }

  public String createQueue(Channel channel, String key, boolean durable, 
			    boolean exclusive, boolean autoDelete) 
    throws Exception {
    return createQueue(channel, key, durable, exclusive, autoDelete, null);
  }

  public String createQueue(Channel channel, String key, boolean durable, 
			    boolean exclusive, boolean autoDelete, 
			    Map<String, Object> args) 
    throws Exception {
    String queueName = getQueueName(key);
    channel.queueDeclare(queueName, durable, exclusive, autoDelete, args);
    channel.queueBind(queueName, getExchangeName(key), getRoutingKey(key));
    return queueName;
  }

  public String getRoutingKey() {
    return getRoutingKey(null);
  }

  public String getRoutingKey(String key) {
    String rk = getValueOrDefault("queue_routing_key", key);
    return (rk != null) ? rk : DEFAULT_ROUTING_KEY; 
  }

  public String getExchangeName(String key) {
    return getValueOrDefault("exchange_name", key);
  }

  public String getExchangeType(String key) {
    return getValueOrDefault("exchange_type", key);
  }

  public String getQueueName(String key) {
    return getValueOrDefault("queue_name", key);
  }

  public Address[] getMultipleHosts() {
    Iterable<String> hosts = conf.getIterable("amqp_hosts");
    if (hosts == null) {
      return null;
    }
    List<Address> addressList = new ArrayList<Address>();
    for (String host : hosts) {
      if (host != null) {
	addressList.add(new Address(host));
      }
    }
    if (addressList.size() > 0) {
      return addressList.toArray(new Address[0]);
    }
    return null;
  }

  private String getValueOrDefault(String type, String key) {
    return conf.getString(type + ((key != null) ? "_" +  key : ""));

  }

  private Integer getIntegerOrDefault(String type, String key) {
    return conf.getInteger(type + ((key != null) ? "_" +  key : ""));
  }

  private Boolean getBooleanOrDefault(String type, String key) {
    return conf.getBoolean(type + ((key != null) ? "_" +  key : ""));
  }

  public QueueingConsumer createQueueingConsumer(Channel channel, 
                                                    String queue) 
    throws Exception {
    QueueingConsumer consumer = new QueueingConsumer(channel);
    channel.basicConsume(queue, false, consumer);
    return consumer;
  }

  public byte[] getBodyAndAck(Channel channel, QueueingConsumer consumer) 
    throws Exception {
      QueueingConsumer.Delivery delivery = consumer.nextDelivery();  
      byte[] body = delivery.getBody();   
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();       
      channel.basicAck(deliveryTag, true);
      return body;
  }

  public void ack(Channel channel, QueueingConsumer.Delivery delivery) 
    throws Exception {
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();       
      channel.basicAck(deliveryTag, true);
  }

  public void reject(Channel channel, QueueingConsumer.Delivery delivery) 
    throws Exception {
      long deliveryTag = delivery.getEnvelope().getDeliveryTag();       
      channel.basicReject(deliveryTag, true);
  }
}