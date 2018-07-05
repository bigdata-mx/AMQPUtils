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

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import com.google.common.base.MoreObjects;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import com.rabbitmq.client.MessageProperties;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;

import org.apache.log4j.Logger;

import mx.bigdata.utils.amqp.AMQPClientHelper;

public final class HaPublisher extends ReconnectingPublisher {  

  private final static int DEFAULT_HEARTBEAT_SECONDS = 50;

  private final Logger logger = Logger.getLogger(getClass());

  private volatile SortedMap<Long, PublishWrapper> unconfirmedMap =
    Collections.synchronizedSortedMap(new TreeMap<Long, PublishWrapper>());
  
  public HaPublisher(String tag, String key, AMQPClientHelper amqp, 
                     ConnectionFactory factory) throws Exception {
    super(tag, key, amqp, factory);
    if (factory.getRequestedHeartbeat() == 0) {
      factory.setRequestedHeartbeat(DEFAULT_HEARTBEAT_SECONDS);
    }
    initConfirms();
  }
  
  private void initConfirms() throws Exception {
    getChannel().addConfirmListener(new ConfirmListener() {
        public void handleAck(long seqNo, boolean multiple) {
          if (multiple) {
            Map<Long, PublishWrapper> headMap = unconfirmedMap.headMap(seqNo+1);
            logger.trace(tag + " confirmed " + headMap.size() 
                         + " messages up to " + seqNo + " sequence number");
            headMap.clear();
          } else {
            logger.trace(tag + " confirmed 1 messages with " + seqNo 
                         + " sequence number");
            unconfirmedMap.remove(seqNo);
          }
        }
	
        public void handleNack(long seqNo, boolean multiple) {
          if (multiple) {
            Iterator<Map.Entry<Long, PublishWrapper>> entries = unconfirmedMap
              .headMap(seqNo+1).entrySet().iterator();
            while (entries.hasNext()) {
              Map.Entry<Long, PublishWrapper> entry = entries.next();
              try {
                entry.getValue().publish();
              } catch (Exception ex) {
                logger.error(tag + " unable to republish unconfirmed message: " 
                             + entry.getKey());
              }
              entries.remove();
            }
          } else {
            try {
              unconfirmedMap.remove(seqNo).publish();
            } catch (Exception ex) {
              logger.error(tag + " unable to republish unconfirmed message: " 
                           + seqNo);
            }
          }
        }
      });
    getChannel().confirmSelect();
  }

  public HaPublisher(String tag, String key, AMQPClientHelper amqp) 
    throws Exception {
    this(tag, key, amqp, amqp.createConnectionFactory(key));
  }

  @Override
  public synchronized void publish(String exch, String routingKey, 
                                   boolean mandatory, boolean immediate, 
                                   AMQP.BasicProperties props, 
                                   byte[] bytes) throws IOException {
    // if (props == null) {
    //   props = MessageProperties.PERSISTENT_BASIC;
    // }
    PublishWrapper msg = 
      new PublishWrapper(exch, routingKey, mandatory, immediate, props, bytes);
    logger.trace("Publishing: " + msg);
    unconfirmedMap.put(getChannel().getNextPublishSeqNo(), msg);
    msg.publish();
    //publish(exch, routingKey, mandatory, immediate, props, bytes);
  }

  private final class PublishWrapper {

    private final String exch;
    private final String routingKey;
    private final boolean mandatory;
    private final boolean immediate;
    private final AMQP.BasicProperties props;
    private final byte[] bytes;

    PublishWrapper(String exch, String routingKey, boolean mandatory, 
                   boolean immediate, AMQP.BasicProperties props, 
                   byte[] bytes) {
      this.exch = exch; 
      this.routingKey = routingKey; 
      this.mandatory = mandatory; 
      this.immediate = immediate; 
      this.props = props; 
      this.bytes = bytes;
    }
    
    void publish() throws IOException {
      HaPublisher.super
        .publish(exch, routingKey, mandatory, immediate, props, bytes);
    }
    
    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
        .add("exchange", exch).add("routing-key", routingKey)
        .add("mandatory", mandatory).add("immediate", immediate)
        .add("props", props).toString();
    }
  }
}
