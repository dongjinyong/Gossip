package com.dongjinyong.gossip.net;
/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */


import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class IncomingTcpConnection extends Thread
{
    private static Logger logger = LoggerFactory.getLogger(IncomingTcpConnection.class);

    private static final int CHUNK_SIZE = 1024 * 1024;

    private Socket socket;
    public InetAddress from;

    public IncomingTcpConnection(Socket socket)
    {
        assert socket != null;
        this.socket = socket;
    }

    /**
     * A new connection will either stream or message for its entire lifetime: because streaming
     * bypasses the InputStream implementations to use sendFile, we cannot begin buffering until
     * we've determined the type of the connection.
     */
    @Override
    public void run()
    {
        DataInputStream input;
        int version;
        try
        {
            // determine the connection type to decide whether to buffer
            input = new DataInputStream(socket.getInputStream());
            
            // loop to get the next message.
            while (true)
            {
                MessagingService.validateMagic(input.readInt());
            	
            	//int header1 = input.readInt();        //读第2个
            	
                int totalSize = input.readInt();       //读第3个
                String id = input.readUTF();      //读第4个
                Header header = Header.serializer().deserialize(input);  //读第5个
               
//                Gossiper.instance.setVersion(header.getFrom(), version);   //Receive the first message to set the version.
                
                int bodySize = input.readInt();     //读第6个
                
                byte[] body = new byte[bodySize];
                // readFully allocates a direct buffer the size of the chunk it is asked to read,
                // so we cap that at CHUNK_SIZE.  See https://issues.apache.org/jira/browse/CASSANDRA-2654
                int remainder = bodySize % CHUNK_SIZE;
                for (int offset = 0; offset < bodySize - remainder; offset += CHUNK_SIZE)
                    input.readFully(body, offset, CHUNK_SIZE);
                input.readFully(body, bodySize - remainder, remainder);
                // earlier versions would send unnecessary bytes left over at the end of a buffer, too
                long remaining = totalSize - MessagingService.messageLength(header, id, body);
                
                while (remaining > 0)
                    remaining -= input.skip(remaining);

                // for non-streaming connections, continue to read the messages (and ignore them) until sender
                // starts sending correct-version messages (which it can do without reconnecting -- version is per-Message)
//                if (version <= MessagingService.version_) //待理解
                if (true)
                {
                    Message message = new Message(header, body);
//                  MessagingService.instance().receive(message, id);
                    //即：
                    MessageVerb.Verb verb = message.getVerb();

                    IVerbHandler verbHandler = MessagingService.instance().getVerbHandler(verb);
                    if (verbHandler == null)
                    {
                        logger.debug("Unknown verb {}", verb);
                    }

//                    System.out.println("receive message,doVerb. Message type:"+verb+",message len:"+bodySize+",message from:"+message.getFrom());
                    verbHandler.doVerb(message, id);
                }
                else
                {
                    logger.debug("Received connection from newer protocol version {}. Ignoring message", version);
                }
               
             }
           
        }
        catch (EOFException e)
        {
            logger.trace("eof reading from socket; closing", e);
            // connection will be reset so no need to throw an exception.
        }
        catch (IOException e)
        {
            logger.debug("IOError reading from socket; closing", e);
        }
        finally
        {
            close();
        }
    }


    void close()
    {
        try
        {
            socket.close();
        }
        catch (IOException e)
        {
            if (logger.isDebugEnabled())
                logger.debug("error closing socket", e);
        }
    }

}



