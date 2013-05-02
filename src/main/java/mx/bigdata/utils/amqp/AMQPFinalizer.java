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

import java.util.Set;

import com.google.common.collect.Sets;

import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import org.apache.log4j.Logger;

public final class AMQPFinalizer {

  private final Logger logger = Logger.getLogger(getClass());

  private final Set<Connection> connections = Sets.newHashSet();

  private static AMQPFinalizer instance;

  private AMQPFinalizer() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
	@Override
	public void run() {
	  closeConnections();
	}
      });
  }

  public static synchronized AMQPFinalizer getInstance() {
    if (instance == null) {
      instance = new AMQPFinalizer();
    }
    return instance;
  }

  public synchronized void registerConnection(Connection conn) {
    this.connections.add(conn);
  }

  public synchronized void registerChannel(Channel channel) {
    registerConnection(channel.getConnection());
  }

  public synchronized void closeConnections() {
    for (Connection conn : this.connections) {
      conn.abort(10000);
      logger.warn("Connection closed: " + conn);
    }
    this.connections.clear();
  }
}
