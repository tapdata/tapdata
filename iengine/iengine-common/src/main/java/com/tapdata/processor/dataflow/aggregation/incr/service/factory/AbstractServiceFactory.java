package com.tapdata.processor.dataflow.aggregation.incr.service.factory;

import com.tapdata.entity.Connections;
import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.dataflow.Stage;
import com.tapdata.processor.dataflow.ProcessorContext;
import com.tapdata.processor.dataflow.aggregation.incr.service.ServiceFactory;

import java.util.Optional;

abstract public class AbstractServiceFactory<T> implements ServiceFactory<T> {

	@Override
	public T create(Stage stage, ProcessorContext processorContext) {
		final Connections connections = processorContext.getTargetConn();
		final DatabaseTypeEnum databaseTypeEnum = Optional.ofNullable(DatabaseTypeEnum.fromString(connections.getDatabase_type())).orElseThrow(() -> new IllegalArgumentException(String.format("unknown database type: %s", connections.getDatabase_type())));
		T t = this.doCreate(stage, processorContext, databaseTypeEnum);
		if (t == null) {
			throw new IllegalArgumentException(String.format("unsupported database type: %s", connections.getDatabase_type()));
		}
		return t;
	}

	abstract protected T doCreate(Stage stage, ProcessorContext processorContext, DatabaseTypeEnum databaseTypeEnum);

}
