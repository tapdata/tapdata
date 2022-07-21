package com.tapdata.processor.dataflow.aggregation.incr.service.factory;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.SnapshotService;
import com.tapdata.processor.dataflow.aggregation.incr.service.mongo.MongoSnapshotServiceImpl;

public class SnapshotServiceFactory extends AbstractServiceFactory<SnapshotService> {

	@Override
	public Class<SnapshotService> getServiceType() {
		return SnapshotService.class;
	}

	@Override
	protected SnapshotService doCreate(Stage stage, ProcessorContext processorContext, DatabaseTypeEnum databaseTypeEnum) {
		switch (databaseTypeEnum) {
			case MONGODB:
			case ALIYUN_MONGODB:
				return new MongoSnapshotServiceImpl(stage, processorContext);
		}
		return null;
	}

}
