package io.tapdata.typemapping;

import com.tapdata.mongo.ClientMongoOperator;

/**
 * @author samuel
 * @Description
 * @create 2021-08-05 11:34
 **/
public class TypeMappingFactory {

	public static BaseTypeMapping getTypeMappingJobEngine() throws Exception {
		return new TypeMappingJobEngine();
	}

	public static BaseTypeMapping getTypeMappingJobEngine(ClientMongoOperator clientMongoOperator) throws Exception {
		return new TypeMappingJobEngine(clientMongoOperator);
	}
}
