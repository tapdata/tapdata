package com.tapdata.processor.dataflow.aggregation.incr.convert;

import com.tapdata.constant.ConnectorConstant;

public enum MessageOp {

	INSERT(ConnectorConstant.MESSAGE_OPERATION_INSERT),
	UPDATE(ConnectorConstant.MESSAGE_OPERATION_UPDATE),
	DELETE(ConnectorConstant.MESSAGE_OPERATION_DELETE);

	private final String type;

	MessageOp(String type) {
		this.type = type;
	}

	public String getType() {
		return type;
	}
}
