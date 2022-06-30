package com.tapdata.entity;

public enum ApmLog {

	Log_001("Received query result: {}, FetchSize: {}, FetchDirection: {}, query spent: {} seconds, ReadBatchSize: {}"),
	Log_002("Read {} rows from table {}, all spent: {} ms, adapt spent: {} ms"),
	Log_003("Put into message queue spent: {} ms, rows: {}, batch size: {}, batch count: {}, message queue size: {}"),
	Log_004("Batch process spent: {}, thread name: {}"),
	Log_005("Start transform at {}"),
	Log_006("All transformer threads completed at {}, spent {} seconds, {} rows/second"),
	Log_007("Putting cached list in transform queue spent: {} ms, tableMsgMap size: {}, queue size: {}"),
	Log_008("Preparing write to mongo spent: {} ms, row count: {}, table name: {}, thread name: {}"),
	Log_009("BulkWrite spent: {} ms, write size: {}, thread name: {}, msgindex: {}, upsertResultMap {}"),
	Log_010("Statistics write result spent: {} ms, process count: {}, insert count: {}, update count: {}, delete count: {}"),
	Log_011("Generate fake Bulk write result spent: {} ms"),
	Log_012("Executing query: {}."),
	Log_013("Finished read after data from {} one time, read count: {}, spent: {}ms."),

	Tran_Mongo_Log_001("Dispatched {} msgs spent {} ms"),
	Log_014("Putting cached list in transform queue spent: {} ms,  queue size: {}"),
	;

	private final String msg;

	private final static String PREFIX = "PERF - ";

	ApmLog(String msg) {
		this.msg = PREFIX + msg;
	}

	public String getMsg() {
		return msg;
	}
}
