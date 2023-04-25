package io.tapdata.mongodb.writer.error;

import io.tapdata.mongodb.writer.error.handler.Code11000Handler;
import io.tapdata.mongodb.writer.error.handler.Code28Handler;

import java.util.HashMap;
import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2023-04-23 17:14
 **/
public enum BulkWriteErrorCodeHandlerEnum {
	CODE_28(28, new Code28Handler()),
	CODE_11000(11000, new Code11000Handler()),
	;
	private final int code;
	private final BulkWriteErrorHandler bulkWriteErrorHandler;

	BulkWriteErrorCodeHandlerEnum(int code, BulkWriteErrorHandler bulkWriteErrorHandler) {
		this.code = code;
		this.bulkWriteErrorHandler = bulkWriteErrorHandler;
	}

	public int getCode() {
		return code;
	}

	public BulkWriteErrorHandler getBulkWriteErrorHandler() {
		return bulkWriteErrorHandler;
	}

	private static Map<String, BulkWriteErrorCodeHandlerEnum> codeMap = new HashMap<>();

	static {
		for (BulkWriteErrorCodeHandlerEnum bulkWriteErrorCodeHandlerEnum : BulkWriteErrorCodeHandlerEnum.values()) {
			codeMap.put(String.valueOf(bulkWriteErrorCodeHandlerEnum.code), bulkWriteErrorCodeHandlerEnum);
		}
	}

	public static BulkWriteErrorCodeHandlerEnum fromCode(int code) {
		return codeMap.get(String.valueOf(code));
	}
}
