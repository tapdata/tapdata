package io.tapdata.typemapping;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;

/**
 * @author samuel
 * @Description
 * @create 2021-08-05 11:34
 **/
public class TypeMappingUtil {

	private final static String SKIP_ENV_KEY = "skipTpm";
	private static Logger logger = LogManager.getLogger(TypeMappingUtil.class);

	public static void initJobEngineTypeMappings(ClientMongoOperator clientMongoOperator) throws Exception {
		Map<String, String> env = System.getenv();
		boolean skip = false;
		if (env.containsKey(SKIP_ENV_KEY)) {
			try {
				skip = Boolean.parseBoolean(env.get(SKIP_ENV_KEY));
			} catch (Exception ignored) {
			}
		}
		if (skip) {
			logger.info("Skip init type mappings");
			return;
		}
		BaseTypeMapping typeMappingJobEngine = TypeMappingFactory.getTypeMappingJobEngine(clientMongoOperator);
		typeMappingJobEngine.initTypeMappings();
	}

	public static Class<TypeMappingProvider> getJobEngineTypeMappingClazz(DatabaseTypeEnum databaseTypeEnum) throws Exception {
		BaseTypeMapping typeMappingJobEngine = TypeMappingFactory.getTypeMappingJobEngine();
		return typeMappingJobEngine.getTypeMappingClazzByDatabaseType(databaseTypeEnum);
	}
}
