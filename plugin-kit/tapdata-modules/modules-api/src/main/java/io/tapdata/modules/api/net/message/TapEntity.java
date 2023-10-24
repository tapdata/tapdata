package io.tapdata.modules.api.net.message;

import io.tapdata.entity.serializer.JavaCustomSerializer;

public interface TapEntity extends JavaCustomSerializer {
	default String contentType() {
		return this.getClass().getSimpleName();
	}
}
