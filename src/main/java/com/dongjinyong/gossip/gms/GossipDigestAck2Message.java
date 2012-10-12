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

package com.dongjinyong.gossip.gms;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

import com.dongjinyong.gossip.io.IVersionedSerializer;


/**
 * This message gets sent out as a result of the receipt of a GossipDigestAckMessage. This the
 * last stage of the 3 way messaging of the Gossip protocol.
 */

class GossipDigestAck2Message
{
    private static  IVersionedSerializer<GossipDigestAck2Message> serializer_;
    static
    {
        serializer_ = new GossipDigestAck2MessageSerializer();
    }

    Map<InetSocketAddress, EndpointState> epStateMap_ = new HashMap<InetSocketAddress, EndpointState>();

    public static IVersionedSerializer<GossipDigestAck2Message> serializer()
    {
        return serializer_;
    }

    GossipDigestAck2Message(Map<InetSocketAddress, EndpointState> epStateMap)
    {
        epStateMap_ = epStateMap;
    }

    Map<InetSocketAddress, EndpointState> getEndpointStateMap()
    {
         return epStateMap_;
    }
}

class GossipDigestAck2MessageSerializer implements IVersionedSerializer<GossipDigestAck2Message>
{
    public void serialize(GossipDigestAck2Message gDigestAck2Message, DataOutput dos) throws IOException
    {
        /* Use the EndpointState */
        EndpointStatesSerializationHelper.serialize(gDigestAck2Message.epStateMap_, dos);
    }

    public GossipDigestAck2Message deserialize(DataInput dis) throws IOException
    {
        Map<InetSocketAddress, EndpointState> epStateMap = EndpointStatesSerializationHelper.deserialize(dis);
        return new GossipDigestAck2Message(epStateMap);
    }

    public long serializedSize(GossipDigestAck2Message gossipDigestAck2Message)
    {
        throw new UnsupportedOperationException();
    }
}

