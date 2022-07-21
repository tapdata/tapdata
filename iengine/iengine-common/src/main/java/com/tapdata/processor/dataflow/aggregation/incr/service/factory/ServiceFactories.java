package com.tapdata.processor.dataflow.aggregation.incr.service.factory;

import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.ServiceFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class ServiceFactories {

	private static final Map<Class<?>, ServiceFactory<?>> SERVICE_FACTORY_MAP = new HashMap<>();

	static {
		for (ServiceFactory<?> f : Arrays.asList(new SnapshotServiceFactory(), new AggregationServiceFactory(), new SyncVersionServiceFactory())) {
			SERVICE_FACTORY_MAP.put(f.getServiceType(), f);
		}
	}

	public static <T> T create(Class<?> serviceType, Stage stage, ProcessorContext processorContext) {
		return (T) Optional.ofNullable(SERVICE_FACTORY_MAP.get(serviceType)).map(f -> f.create(stage, processorContext)).orElseThrow(() -> new IllegalArgumentException(String.format("unknown service type: %s", serviceType.getName())));
	}


}
