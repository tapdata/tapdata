package com.tapdata.processor.dataflow.aggregation.incr.service.factory;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.AggregationService;
import com.tapdata.processor.dataflow.aggregation.incr.service.mongo.MongoAggregationServiceImpl;

public class AggregationServiceFactory extends AbstractServiceFactory<AggregationService> {

	@Override
	protected AggregationService doCreate(Stage stage, ProcessorContext processorContext, DatabaseTypeEnum databaseTypeEnum) {
		switch (databaseTypeEnum) {
			case MONGODB:
			case ALIYUN_MONGODB:
				return new MongoAggregationServiceImpl(processorContext);
		}
		return null;
	}

	@Override
	public Class<AggregationService> getServiceType() {
		return AggregationService.class;
	}

}
