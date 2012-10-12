package com.dongjinyong.gossip.config;



public class GossipConfig
{
	public GossipConfig(){
	}
	
	public GossipConfig(String listen_address, String seeds) {
		this.listen_address = listen_address;
		this.seeds = seeds;
	}

	public GossipConfig(String cluster_name, Integer phi_convict_threshold,
			Long rpc_timeout_in_ms, Integer ring_delay_in_ms,
			String listen_address, String seeds) {
		this.cluster_name = cluster_name;
		this.phi_convict_threshold = phi_convict_threshold;
		this.rpc_timeout_in_ms = rpc_timeout_in_ms;
		this.ring_delay_in_ms = ring_delay_in_ms;
		this.listen_address = listen_address;
		this.seeds = seeds;
	}
	
	/**
	 * @return the cluster_name
	 */
	public String getCluster_name() {
		return cluster_name;
	}

	/**
	 * @param cluster_name the cluster_name to set
	 */
	public void setCluster_name(String cluster_name) {
		this.cluster_name = cluster_name;
	}

	/**
	 * @return the phi_convict_threshold
	 */
	public Integer getPhi_convict_threshold() {
		return phi_convict_threshold;
	}

	/**
	 * @param phi_convict_threshold the phi_convict_threshold to set
	 */
	public void setPhi_convict_threshold(Integer phi_convict_threshold) {
		this.phi_convict_threshold = phi_convict_threshold;
	}

	/**
	 * @return the rpc_timeout_in_ms
	 */
	public Long getRpc_timeout_in_ms() {
		return rpc_timeout_in_ms;
	}

	/**
	 * @param rpc_timeout_in_ms the rpc_timeout_in_ms to set
	 */
	public void setRpc_timeout_in_ms(Long rpc_timeout_in_ms) {
		this.rpc_timeout_in_ms = rpc_timeout_in_ms;
	}

	/**
	 * @return the ring_delay_in_ms
	 */
	public Integer getRing_delay_in_ms() {
		return ring_delay_in_ms;
	}

	/**
	 * @param ring_delay_in_ms the ring_delay_in_ms to set
	 */
	public void setRing_delay_in_ms(Integer ring_delay_in_ms) {
		this.ring_delay_in_ms = ring_delay_in_ms;
	}

	/**
	 * @return the listen_address
	 */
	public String getListen_address() {
		return listen_address;
	}

	/**
	 * @param listen_address the listen_address to set
	 */
	public void setListen_address(String listen_address) {
		this.listen_address = listen_address;
	}

	/**
	 * @return the seeds
	 */
	public String getSeeds() {
		return seeds;
	}

	/**
	 * @param seeds the seeds to set
	 */
	public void setSeeds(String seeds) {
		this.seeds = seeds;
	}
	
	private String cluster_name = "Active Cluster";
	private Integer phi_convict_threshold = 8 ;
	//发送消息超时时间。
	//1.连接建立失败时，超过此时间不再重试建立连接。
	//2.消息队列中超过这个时间不再发送。
	//3.故障检测时，同一个节点发来的消息，若时间间隔超过这个值，为了让这个节点尽快失效，不计入故障检测队列，故障检测队列只更新最后达到时间。
	private Long rpc_timeout_in_ms = new Long(10000) ;  //参考cassandra配置默认值
	//环的稳定时间，超过这个时间认为环已经达到稳定状态。
	//用于节点删除、维护justRemovedEndpoints节点的超时时间、以及endpointStateMap真正移除，触发onRemove事件的时间。
	private Integer ring_delay_in_ms = new Integer(30 * 1000);
	private String listen_address ;  //"localhost:9001";
	private String seeds ;   //"localhost:9001";

}

