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

import org.apache.log4j.Logger;

import com.dongjinyong.gossip.config.GossipConfig;
import com.dongjinyong.gossip.config.GossiperDescriptor;
import com.dongjinyong.gossip.gms.ApplicationState;
import com.dongjinyong.gossip.gms.Gossiper;
import com.dongjinyong.gossip.gms.VersionedValue.VersionedValueFactory;
import com.dongjinyong.gossip.locator.EndpointSnitch;
import com.dongjinyong.gossip.locator.IApplicationStateStarting;
import com.dongjinyong.gossip.net.MessagingService;
import com.dongjinyong.gossip.net.NetSnitch;
import com.dongjinyong.gossip.utils.FBUtilities;


/**
 * This class supports two methods for creating a Cassandra node daemon, 
 * invoking the class's main method, and using the jsvc wrapper from 
 * commons-daemon, (for more information on using this class with the 
 * jsvc wrapper, see the 
 * <a href="http://commons.apache.org/daemon/jsvc.html">Commons Daemon</a>
 * documentation).
 */

public class GossipService
{
    private static Logger logger = Logger.getLogger(GossipService.class);
    
    static{
    	Gossiper.instance.register(new NetSnitch());  
    }
    
    public Gossiper getGossiper(){
    	return Gossiper.instance;
    }
    
    public void start(int generationNbr) throws IOException
    {
    	//打开gossip相关的Tcp连接服务，当前项目gossip所用通信暂且独立，没用到其它通信模块。
    	MessagingService.instance().listen(FBUtilities.getLocalAddress());
    	logger.info("Gossiper message service has been started...");
    	logger.info("Gossip starting up...");
    	Gossiper.instance.start(generationNbr); 
    	logger.info("Gossip has been started...");
    }
    

    public void stop() throws IOException, InterruptedException
    {
    	Gossiper.instance.stop(); 
    	logger.info("Gossip has been stoped...");

    	//关闭gossip相关的Tcp连接，当前项目gossip所用通信暂且独立，没用到其它通信模块。
    	MessagingService.instance().shutdownAllConnections();
    	logger.info("All Gossiper connection has been closed...");
    }
    
    
    
    public static void main(String[] args) throws IOException, InterruptedException{
		GossiperDescriptor.init(new GossipConfig("localhost:9001","localhost:9001"));
		
    	final GossipService service = new GossipService();
   	
    	service.getGossiper().register(new EndpointSnitch());
    	service.getGossiper().register(new IApplicationStateStarting(){
    		public void gossiperStarting(){
    	    	service.getGossiper().addLocalApplicationState(ApplicationState.LOAD,VersionedValueFactory.instance.load(7.1));
    	    	service.getGossiper().addLocalApplicationState(ApplicationState.WEIGHT,VersionedValueFactory.instance.weight(5));

    		}
    	});
    	
    	service.start((int)(System.currentTimeMillis() / 1000));
    	
    }
    
}
