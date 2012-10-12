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

package com.dongjinyong.gossip.net;

import java.net.InetSocketAddress;


public class Message
{
    final Header header_;
    private final byte[] body_;

    public Message(Header header, byte[] body)
    {
        assert header != null;
        assert body != null;

        header_ = header;
        body_ = body;
    }

    public Message(InetSocketAddress from, MessageVerb.Verb verb, byte[] body)
    {
        this(new Header(from, verb), body);
    }

  
    public byte[] getMessageBody()
    {
        return body_;
    }


    public InetSocketAddress getFrom()
    {
        return header_.getFrom();
    }


    public MessageVerb.Verb getVerb()
    {
        return header_.getVerb();
    }


    public String toString()
    {
        StringBuilder sbuf = new StringBuilder("");
        String separator = System.getProperty("line.separator");
        sbuf.append("FROM:" + getFrom())
        	.append(separator)
        	.append("VERB:" + getVerb())
        	.append(separator);
        return sbuf.toString();
    }
}
