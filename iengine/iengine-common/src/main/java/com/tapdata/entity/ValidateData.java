package com.tapdata.entity;

import org.bson.types.ObjectId;

import java.util.Map;

/**
 * Created by tapdata on 19/03/2018.
 */
public class ValidateData {

	private ObjectId _id;

	private String table_name;

	private Map<String, Object> data;

	private Map<String, Object> pkCondition;

	private ObjectId msg_id;

	private Map<String, Object> _meta_data;

	public ObjectId get_id() {
		return _id;
	}

	public void set_id(ObjectId _id) {
		this._id = _id;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public Map<String, Object> getData() {
		return data;
	}

	public void setData(Map<String, Object> data) {
		this.data = data;
	}

	public Map<String, Object> getPkCondition() {
		return pkCondition;
	}

	public void setPkCondition(Map<String, Object> pkCondition) {
		this.pkCondition = pkCondition;
	}

	public ObjectId getMsg_id() {
		return msg_id;
	}

	public void setMsg_id(ObjectId msg_id) {
		this.msg_id = msg_id;
	}

	public Map<String, Object> get_meta_data() {
		return _meta_data;
	}

	public void set_meta_data(Map<String, Object> _meta_data) {
		this._meta_data = _meta_data;
	}

	@Override
	public String toString() {
		final StringBuilder sb = new StringBuilder("ValidateData{");
		sb.append("_id=").append(_id);
		sb.append(", tableName='").append(table_name).append('\'');
		sb.append(", data=").append(data);
		sb.append(", pkCondition=").append(pkCondition);
		sb.append(", msg_id=").append(msg_id);
		sb.append(", _meta_data=").append(_meta_data);
		sb.append('}');
		return sb.toString();
	}
}
