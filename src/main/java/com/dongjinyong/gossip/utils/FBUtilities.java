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

package com.dongjinyong.gossip.utils;

import java.net.InetSocketAddress;

import com.dongjinyong.gossip.config.GossiperDescriptor;

public class FBUtilities
{
    private static volatile InetSocketAddress localInetAddress_;
    private static volatile InetSocketAddress broadcastInetAddress_;

    /**
     * Please use getBroadcastAddress instead. You need this only when you have to listen/connect.
     */
    public static InetSocketAddress getLocalAddress()
    {
        if (localInetAddress_ == null){
        	localInetAddress_ = GossiperDescriptor.getListenAddress();
        }
        return localInetAddress_;
    }

    public static InetSocketAddress getBroadcastAddress()
    {
        if (broadcastInetAddress_ == null)
            broadcastInetAddress_ = GossiperDescriptor.getBroadcastAddress() == null
                                ? getLocalAddress()
                                : GossiperDescriptor.getBroadcastAddress();
        return broadcastInetAddress_;
    }



    public static int encodedUTF8Length(String st)
    {
        int strlen = st.length();
        int utflen = 0;
        for (int i = 0; i < strlen; i++)
        {
            int c = st.charAt(i);
            if ((c >= 0x0001) && (c <= 0x007F))
                utflen++;
            else if (c > 0x07FF)
                utflen += 3;
            else
                utflen += 2;
        }
        return utflen;
    }



}
