package com.tapdata.entity;

import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class FieldProcess implements Serializable, Comparable<FieldProcess> {

	private static final long serialVersionUID = -7280359347547476287L;

	public enum FieldOp {
		OP_CREATE("CREATE", 4),
		OP_REMOVE("REMOVE", 2),
		OP_CONVERT("CONVERT", 3),
		OP_RENAME("RENAME", 1),
		;

		String operation;
		Integer sort;

		FieldOp(String operation, Integer sort) {
			this.operation = operation;
			this.sort = sort;
		}

		private static final Map<String, FieldOp> map = new HashMap<>();

		static {
			for (FieldOp fieldOp : FieldOp.values()) {
				map.put(fieldOp.operation, fieldOp);
			}
		}

		public static FieldOp fromOperation(String operation) {
			return map.get(operation);
		}

		public int getSort() {
			return sort;
		}

		public String getOperation() {
			return operation;
		}
	}

	private String field;

	/**
	 * RENAME/REMOVE/CONVERT
	 */
	private String op;

	private String operand;

	private String javaType;

	private String originedatatype;

	private String originalDataType;

	public String getField() {
		return field;
	}

	public void setField(String field) {
		this.field = field;
	}

	public String getOp() {
		return op;
	}

	public void setOp(String op) {
		this.op = op;
	}

	public String getOperand() {
		return operand;
	}

	public void setOperand(String operand) {
		this.operand = operand;
	}

	public String getOriginedatatype() {
		return originedatatype;
	}

	public void setOriginedatatype(String originedatatype) {
		this.originedatatype = originedatatype;
	}

	public String getOriginalDataType() {
		return originalDataType;
	}

	public void setOriginalDataType(String originalDataType) {
		this.originalDataType = originalDataType;
		if (StringUtils.isBlank(originalDataType) && StringUtils.isNotBlank(originalDataType)) {
			this.originedatatype = originalDataType;
		}
	}

	public String getJavaType() {
		return javaType;
	}

	public void setJavaType(String javaType) {
		this.javaType = javaType;
	}

	@Override
	public int compareTo(FieldProcess fieldProcess) {

		String op1 = fieldProcess.getOp();
		FieldOp fieldOp1 = FieldOp.fromOperation(op1);
		FieldOp fieldOp = FieldOp.fromOperation(op);

		if (fieldOp == null) {
			return -1;
		}

		if (fieldOp1 == null) {
			return 1;
		}

		Integer sort1 = fieldOp1.sort;
		Integer sort = fieldOp.sort;


		return sort.compareTo(sort1);
	}
}
