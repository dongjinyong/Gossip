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

package com.dongjinyong.gossip.service;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import com.dongjinyong.gossip.concurrent.DebuggableScheduledThreadPoolExecutor;
import com.dongjinyong.gossip.config.GossipConfig;
import com.dongjinyong.gossip.config.GossiperDescriptor;
import com.dongjinyong.gossip.gms.ApplicationState;
import com.dongjinyong.gossip.gms.EndpointState;
import com.dongjinyong.gossip.gms.Gossiper;
import com.dongjinyong.gossip.gms.VersionedValue;
import com.dongjinyong.gossip.gms.VersionedValue.VersionedValueFactory;
import com.dongjinyong.gossip.locator.EndpointSnitch;
import com.dongjinyong.gossip.locator.IApplicationStateStarting;


/**
 * This class supports two methods for creating a Cassandra node daemon, 
 * invoking the class's main method, and using the jsvc wrapper from 
 * commons-daemon, (for more information on using this class with the 
 * jsvc wrapper, see the 
 * <a href="http://commons.apache.org/daemon/jsvc.html">Commons Daemon</a>
 * documentation).
 */

public class DaemonTest3
{
    private void printEndpointStates() throws IOException
    {
        DebuggableScheduledThreadPoolExecutor executor = new DebuggableScheduledThreadPoolExecutor("abc");
        executor.scheduleWithFixedDelay(new Runnable(){
        	public void run(){
        		
        		Set<Entry<InetSocketAddress, EndpointState>> set =  Gossiper.instance.getEndpointStates();
        		for (Iterator<Entry<InetSocketAddress, EndpointState>>  iterator = set.iterator(); iterator.hasNext();) {
					Entry<InetSocketAddress, EndpointState> entry = iterator.next();
					System.out.println("key:"+entry.getKey()+", value:"+entry.getValue());
					
					EndpointState endpoint = entry.getValue();
					for (Entry<ApplicationState,VersionedValue>  entry2 : endpoint.getApplicationStateMapEntrySet()) {
						System.out.println("VersionedValue----key:"+entry2.getKey()+", value:"+entry2.getValue());
					}
				}
        		System.out.println("=======================");
        		
        		Set<InetSocketAddress> liveset =  Gossiper.instance.getLiveMembers();
        		for (Iterator<InetSocketAddress> iterator = liveset.iterator(); iterator.hasNext();) {
        			InetSocketAddress inetAddress = (InetSocketAddress) iterator.next();
					System.out.println(inetAddress);
				}
        	}
        },
        Gossiper.intervalInMillis,
        Gossiper.intervalInMillis,
        TimeUnit.MILLISECONDS);
        
    }
    
    public static void main(String[] args) throws IOException
    {
    	
		GossiperDescriptor.init(new GossipConfig("localhost:9003","localhost:9001,localhost:9002"));
		//TODO
    	final GossipService service = new GossipService();
       	
    	service.getGossiper().register(new EndpointSnitch());
    	service.getGossiper().register(new IApplicationStateStarting(){
    		public void gossiperStarting(){
    	    	service.getGossiper().addLocalApplicationState(ApplicationState.LOAD,VersionedValueFactory.instance.load(7.1));
    	    	service.getGossiper().addLocalApplicationState(ApplicationState.WEIGHT,VersionedValueFactory.instance.weight(5));

    		}
    	});
    	
    	service.start((int)(System.currentTimeMillis() / 1000));
    	
//    	new DaemonTest3().printEndpointStates();
    	
    	
    }
}
