package com.dongjinyong.gossip.net;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


import java.net.InetSocketAddress;


public class OutboundTcpConnectionPool
{
    // pointer for the real Address.
    private final InetSocketAddress id;
    public final OutboundTcpConnection ackCon;

    OutboundTcpConnectionPool(InetSocketAddress remoteEp)
    {
        id = remoteEp;
        ackCon = new OutboundTcpConnection(this);
        ackCon.start();
    }

    /**
     * returns the appropriate connection based on message type.
     * returns null if a connection could not be established.
     */
    OutboundTcpConnection getConnection(Message msg)
    {
        return ackCon;
    }

    InetSocketAddress endPoint()
    {
        return id;
    }

    synchronized void reset()
    {
        ackCon.closeSocket();
    }

    //added by jydong
    synchronized void shutdown()    
    {
    	if(ackCon.isAlive()){
    		ackCon.shutdownSocketThread();
    	}
    }
    

}
