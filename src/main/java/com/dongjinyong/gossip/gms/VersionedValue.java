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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.dongjinyong.gossip.io.IVersionedSerializer;


/**
 * This abstraction represents the state associated with a particular node which an
 * application wants to make available to the rest of the nodes in the cluster.
 * Whenever a piece of state needs to be disseminated to the rest of cluster wrap
 * the state in an instance of <i>ApplicationState</i> and add it to the Gossiper.
 *
 * e.g. if we want to disseminate load information for node A do the following:
 *
 *      ApplicationState loadState = new ApplicationState(<string representation of load>);
 *      Gossiper.instance.addApplicationState("LOAD STATE", loadState);
 */

public class VersionedValue implements Comparable<VersionedValue>
{
    public static final IVersionedSerializer<VersionedValue> serializer = new VersionedValueSerializer();

    // this must be a char that cannot be present in any token
    public final static char DELIMITER = ',';
    public final static String DELIMITER_STR = new String(new char[] { DELIMITER });

    // values for ApplicationState.STATUS
    public final static String STATUS_BOOTSTRAPPING = "BOOT";
    public final static String STATUS_NORMAL = "NORMAL";

    public final static String REMOVING_TOKEN = "removing";
    public final static String REMOVED_TOKEN = "removed";

    public final int version;
    public final String value;

    private VersionedValue(String value, int version)
    {
        assert value != null;
        this.value = value;
        this.version = version;
    }

    private VersionedValue(String value)
    {
        this(value, VersionGenerator.getNextVersion());
    }

    public int compareTo(VersionedValue value)
    {
        return this.version - value.version;
    }

    @Override
    public String toString()
    {
        return "Value(" + value + "," + version + ")";
    }


    private static class VersionedValueSerializer implements IVersionedSerializer<VersionedValue>
    {
        public void serialize(VersionedValue value, DataOutput dos) throws IOException
        {
            dos.writeUTF(value.value);
            dos.writeInt(value.version);
        }

        public VersionedValue deserialize(DataInput dis) throws IOException
        {
            String value = dis.readUTF();
            int valVersion = dis.readInt();
            return new VersionedValue(value, valVersion);
        }

        public long serializedSize(VersionedValue value)
        {
            throw new UnsupportedOperationException();
        }
    }
    
    
    public static class VersionedValueFactory
    {
    	public static final VersionedValueFactory instance = new VersionedValueFactory();
    	private VersionedValueFactory(){
    	}
    	
    	
        public VersionedValue bootstrapping()
        {
            return new VersionedValue(VersionedValue.STATUS_BOOTSTRAPPING);
        }

        public VersionedValue normal()
        {
            return new VersionedValue(VersionedValue.STATUS_NORMAL);
        }
 
        public VersionedValue removingNonlocal()
        {
            return new VersionedValue(VersionedValue.REMOVING_TOKEN);
        }

        public VersionedValue removedNonlocal(long expireTime)
        {
			return new VersionedValue(VersionedValue.REMOVED_TOKEN + VersionedValue.DELIMITER + expireTime);
        }

        public VersionedValue load(double load)
        {
        	return new VersionedValue(String.valueOf(load));
        }
        
        public VersionedValue weight(int weight)
        {
        	return new VersionedValue(String.valueOf(weight));
        }
        
        public VersionedValue getVersionedValue(String str)
        {
        	return new VersionedValue(str);
        }
        
        
        
    }
    
    
    
}

