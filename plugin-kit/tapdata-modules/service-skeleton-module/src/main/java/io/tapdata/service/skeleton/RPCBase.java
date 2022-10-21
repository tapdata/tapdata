package io.tapdata.service.skeleton;

import io.tapdata.entity.serializer.JavaCustomSerializer;

public abstract class RPCBase implements JavaCustomSerializer {
	private String type;
	
	public RPCBase(String type) {
		this.type = type;
	}
	
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
}
