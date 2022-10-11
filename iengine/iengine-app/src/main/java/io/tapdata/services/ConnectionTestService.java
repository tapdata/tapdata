package io.tapdata.services;

import com.tapdata.entity.Connections;
import io.tapdata.service.skeleton.annotation.RemoteService;

import java.util.Map;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

@RemoteService
public class ConnectionTestService {
	public Map<String, Object> connectionTest(Connections connections) {
		return map(entry("1", 1));
	}

	public void a() {}

}
