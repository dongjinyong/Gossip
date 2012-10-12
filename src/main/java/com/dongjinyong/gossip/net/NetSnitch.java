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


import java.net.InetSocketAddress;

import com.dongjinyong.gossip.gms.ApplicationState;
import com.dongjinyong.gossip.gms.EndpointState;
import com.dongjinyong.gossip.gms.IEndpointStateChangeSubscriber;
import com.dongjinyong.gossip.gms.VersionedValue;
import com.dongjinyong.gossip.net.MessagingService;

/**
 * 1) Snitch will automatically set the public IP by querying the AWS API
 *
 * 2) Snitch will set the private IP as a Gossip application state.
 *
 * 3) Snitch implements IESCS and will reset the connection if it is within the
 * same region to communicate via private IP.
 *
 * Implements Ec2Snitch to inherit its functionality and extend it for
 * Multi-Region.
 *
 * Operational: All the nodes in this cluster needs to be able to (modify the
 * Security group settings in AWS) communicate via Public IP's.
 */


/***
 * jydong add
 */
public class NetSnitch implements IEndpointStateChangeSubscriber
{

	@Override
	public void onAlive(InetSocketAddress endpoint, EndpointState state) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onChange(InetSocketAddress endpoint, ApplicationState state,
			VersionedValue value) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onDead(InetSocketAddress endpoint, EndpointState state) {
		// TODO Auto-generated method stub
		MessagingService.instance().convict(endpoint);  //关闭发送消息的socket连接
	}

	@Override
	public void onJoin(InetSocketAddress endpoint, EndpointState epState) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onRemove(InetSocketAddress endpoint) {
		// TODO Auto-generated method stub
		MessagingService.instance().convict(endpoint);  //关闭发送消息的socket连接
	}

	@Override
	public void onRestart(InetSocketAddress endpoint, EndpointState state) {
		// TODO Auto-generated method stub
	}

}
