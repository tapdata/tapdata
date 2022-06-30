package com.tapdata.entity;

import java.util.List;
import java.util.Map;

/**
 * Created by tapdata on 30/03/2018.
 */
public class MessageQueueItem {

	public static final int MESSAGE_ITEM_CODE_DONE = 3000;

	public static final int MESSAGE_ITEM_CODE_SWITCH_TABLE = 2000;

	public static final int MESSAGE_ITEM_CODE_NORMAL = 1000;

	/**
	 * 1000 normal code
	 * 2000 switch table code
	 * 3000 all done
	 */
	private int code;

	private Long msgBatchNo;

	private Map<String, List<MessageEntity>> msgs;

	public MessageQueueItem(int code, Map<String, List<MessageEntity>> msgs, Long msgBatchNo) {
		this.code = code;
		this.msgs = msgs;
		this.msgBatchNo = msgBatchNo;
	}

	public Map<String, List<MessageEntity>> getMsgs() {
		return msgs;
	}

	public void setMsgs(Map<String, List<MessageEntity>> msgs) {
		this.msgs = msgs;
	}

	public int getCode() {
		return code;
	}

	public void setCode(int code) {
		this.code = code;
	}

	public Long getMsgBatchNo() {
		return msgBatchNo;
	}

	public void setMsgBatchNo(Long msgBatchNo) {
		this.msgBatchNo = msgBatchNo;
	}
}
