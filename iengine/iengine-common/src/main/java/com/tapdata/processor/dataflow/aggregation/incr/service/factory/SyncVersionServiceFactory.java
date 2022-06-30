package com.tapdata.processor.dataflow.aggregation.incr.service.factory;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.SyncVersionService;
import com.tapdata.processor.dataflow.aggregation.incr.service.mongo.MongoSyncVersionServiceImpl;

public class SyncVersionServiceFactory extends AbstractServiceFactory<SyncVersionService> {

	@Override
	protected SyncVersionService doCreate(Stage stage, ProcessorContext processorContext, DatabaseTypeEnum databaseTypeEnum) {
		switch (databaseTypeEnum) {
			case MONGODB:
			case ALIYUN_MONGODB:
				return new MongoSyncVersionServiceImpl(stage, processorContext);
		}
		return null;
	}

	@Override
	public Class<SyncVersionService> getServiceType() {
		return SyncVersionService.class;
	}

}
