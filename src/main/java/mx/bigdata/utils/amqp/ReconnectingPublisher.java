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
import java.util.Set;
import java.util.HashSet;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;

import java.util.logging.Level;
import java.util.logging.Logger;

import mx.bigdata.utils.amqp.AMQPClientHelper;

public class ReconnectingPublisher {  

  private final Logger logger = Logger.getLogger(getClass().getName());

  private final ConnectionFactory factory;

  private final String tag;

  protected final String key;

  protected final AMQPClientHelper amqp;

  protected final Set<String> declaredExchanges = new HashSet();

  private Channel out;

  public ReconnectingPublisher(String tag, String key, AMQPClientHelper amqp, 
			      ConnectionFactory factory) {
    this.tag = tag;
    this.key = key;
    this.amqp = amqp;
    this.factory = factory;
    initPublisher();
  }

  public ReconnectingPublisher(String tag, String key, AMQPClientHelper amqp) 
    throws Exception {
    this(tag, key, amqp, amqp.createConnectionFactory(key));
  }

  private boolean initPublisher() {
    try {
      declaredExchanges.clear();
      String exchName = amqp.getExchangeName(key);
      if (exchName == null || amqp.getExchangeType(key) == null) {
	out = amqp.declareChannel(factory, key, false);
      } else {
	out = amqp.declareChannel(factory, key);
	declaredExchanges.add(exchName);
      }
      out.addShutdownListener(new ShutdownListener() {
	  public void shutdownCompleted(ShutdownSignalException sig) {
	    logger.log(Level.WARNING, "ShutdownSignal for tag: " + tag
		       + "\n\t reason: " + sig.getReason() 
		       + "\n\t reference: " + sig.getReason(), sig);
	    try {
	      out.getConnection().close();
	    } catch(Exception ignore) { }
	    reconnect();
	  }
	});
      logger.info("Publisher " + tag + " initilized");
      return true;
    } catch (Throwable e) {
      logger.fine("Exception initializing publisher " + tag + ": " + e);
      if (out != null) {
	try {
	  out.getConnection().close();
	} catch (Exception ingore) { }
	//out = null;
      }
    }
    return false;
  }

  public void publish(byte[] bytes) throws IOException {
    publish(null, null, bytes);
  }
  
  public void publish(String exchange, String routingKey, byte[] bytes) 
    throws IOException {
    publish(exchange, routingKey, false, false, null, bytes);
  }
  
  public void publish(String exch, String routingKey, boolean mandatory, 
		      boolean immediate, AMQP.BasicProperties props, 
		      byte[] bytes) throws IOException {
    try {
      exch = (exch == null) ? amqp.getExchangeName(key) : exch;
      routingKey = (routingKey == null) ? amqp.getRoutingKey(key) : routingKey;
      if (!declaredExchanges.contains(exch)) {
	out.exchangeDeclare(exch, amqp.getExchangeType(key), true);
	declaredExchanges.add(exch);
      }
      out.basicPublish(exch, routingKey, mandatory, immediate, props, bytes);
    } catch(Exception e) {
      logger.log(Level.WARNING, "Exception while publishing: ", e);
      try {
	out.getConnection().close();
      } catch(Exception ingnore) { }
      reconnect();
      publish(exch, routingKey, mandatory, immediate, props, bytes);
    }
  }
  
  private void reconnect() {
    reconnect(0, 1);
  }

  private void reconnect(int backoff, int pow) {
    try {
      if (backoff > 0) {
	logger.info("Reconnecting consumer " + tag + " in " 
		    + (backoff / 1000) + " seconds ");
	Thread.sleep(backoff);
      }
    } catch (InterruptedException ignore) { }
    boolean initialized = initPublisher();
    if (!initialized) {
      backoff = (int) (Math.pow(2, pow));
      pow += 1;
      reconnect(backoff, pow);
    }
  }
}