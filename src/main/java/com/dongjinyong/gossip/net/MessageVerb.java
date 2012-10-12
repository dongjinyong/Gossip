package com.dongjinyong.gossip.net;


/***
 * 代替cassandra的类
 * @author jydong
 * 2012-6-25
 *
 */
public class MessageVerb {
    
    /* All verb handler identifiers */
    public enum Verb
    {
    	GOSSIP_DIGEST_SYN,
    	GOSSIP_DIGEST_ACK,
    	GOSSIP_DIGEST_ACK2,
    	GOSSIP_SHUTDOWN,
    	INTERNAL_RESPONSE, // responses to internal calls
    	INTERNAL_SHUTDOWN, // 关闭
//    	REQUEST_RESPONSE, // client-initiated reads and writes
//        MUTATION,
//        BINARY, // Deprecated
//        READ_REPAIR,
//        READ,
//        STREAM_INITIATE, // Deprecated
//        STREAM_INITIATE_DONE, // Deprecated
//        STREAM_REPLY,
//        STREAM_REQUEST,
//        RANGE_SLICE,
//        BOOTSTRAP_TOKEN,
//        TREE_REQUEST,
//        TREE_RESPONSE,
//        JOIN, // Deprecated
//        DEFINITIONS_ANNOUNCE, // Deprecated
//        DEFINITIONS_UPDATE,
//        TRUNCATE,
//        SCHEMA_CHECK,
//        INDEX_SCAN, // Deprecated
//        REPLICATION_FINISHED,
//        COUNTER_MUTATION,
//        STREAMING_REPAIR_REQUEST,
//        STREAMING_REPAIR_RESPONSE,
//        SNAPSHOT, // Similar to nt snapshot
//        MIGRATION_REQUEST,
//        // use as padding for backwards compatability where a previous version needs to validate a verb from the future.
//        UNUSED_1,
//        UNUSED_2,
//        UNUSED_3,
        ;
        // remember to add new verbs at the end, since we serialize by ordinal
    }
    public static final Verb[] VERBS = Verb.values();


}
