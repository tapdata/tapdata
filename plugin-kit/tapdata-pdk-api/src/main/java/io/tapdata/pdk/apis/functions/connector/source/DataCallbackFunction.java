package io.tapdata.pdk.apis.functions.connector.source;

import io.tapdata.entity.utils.DataMap;

public interface DataCallbackFunction {
	void callback(DataMap data);
}
