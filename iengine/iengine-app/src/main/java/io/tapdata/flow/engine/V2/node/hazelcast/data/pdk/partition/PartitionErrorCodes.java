package io.tapdata.flow.engine.V2.node.hazelcast.data.pdk.partition;

/**
 * @author aplomb
 */
public interface PartitionErrorCodes {
	int DDL_NOT_ALLOWED_IN_PARTITION = 19001;
	int CONTROL_NOT_ALLOWED_IN_PARTITION = 19002;
	int PARTITION_INDEX_NULL = 19003;
	int PARTITION_NOT_FOUND_FOR_VALUE = 19004;
	int TAP_TABLE_NOT_FOUND = 19005;
}
