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

import java.io.*;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;

public class CompactEndpointSerializationHelper
{
    public static void serialize(InetSocketAddress endpoint, DataOutput dos) throws IOException
    {
        byte[] buf = endpoint.getAddress().getAddress();
        dos.writeByte(buf.length);
        dos.write(buf);
        dos.writeInt(endpoint.getPort());
    }

    public static InetSocketAddress deserialize(DataInput dis) throws IOException
    {
        byte[] bytes = new byte[dis.readByte()];
        dis.readFully(bytes, 0, bytes.length);
        int port = dis.readInt();
        return new InetSocketAddress(InetAddress.getByAddress(bytes),port);
    }

    public static int serializedSize(InetSocketAddress from)
    {
        if (from.getAddress() instanceof Inet4Address)
            return 1 + 4 + 4;
        assert from.getAddress() instanceof Inet6Address;
        return 1 + 16 + 4;
    }
}
