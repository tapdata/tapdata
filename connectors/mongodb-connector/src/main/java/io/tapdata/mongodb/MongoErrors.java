package io.tapdata.mongodb;

/**
 * @author aplomb
 */
public interface MongoErrors {
	int KEY_OUTSIDE_OF_PARTITION_KEYS = 12000;

	int NO_INDEX_FOR_PARTITION = 12001;
	int FIELD_NOT_IN_PARTITION_INDEXES = 12002;
	int NO_RECORD_WHILE_GET_MIN = 12003;
	int MIN_VALUE_IS_NULL = 12004;
	int NO_RECORD_WHILE_GET_MAX = 12005;
	int MAX_VALUE_IS_NULL = 12006;
}
