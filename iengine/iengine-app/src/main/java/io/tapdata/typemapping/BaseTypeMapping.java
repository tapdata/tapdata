package io.tapdata.typemapping;

import com.tapdata.entity.DatabaseTypeEnum;
import com.tapdata.entity.TapType;
import com.tapdata.entity.TypeMapping;
import com.tapdata.mongo.ClientMongoOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Serializable;
import java.util.List;

/**
 * @author samuel
 * @Description
 * @create 2021-08-04 11:31
 **/
public abstract class BaseTypeMapping implements Serializable {

	private static final long serialVersionUID = 9102658158271765640L;
	protected Logger logger = LogManager.getLogger(BaseTypeMapping.class);
	protected List<TypeMapping> typeMappings;
	protected ClientMongoOperator clientMongoOperator;

	protected final static String TYPE_PACKAGE_NAME = "io.tapdata";
	protected final static String TYPE_MAPPING_PROVIDER_CLASS_PATH_NAME = "io.tapdata.typemapping.TypeMappingProvider";
	protected final static String METHOD_PREFIX = "bind";

	public BaseTypeMapping() {
	}

	public BaseTypeMapping(ClientMongoOperator clientMongoOperator) throws Exception {
		this.clientMongoOperator = clientMongoOperator;
	}

	abstract protected List<TypeMapping> initTypeMappings() throws Exception;

	abstract public Class<TypeMappingProvider> getTypeMappingClazzByDatabaseType(DatabaseTypeEnum databaseTypeEnum) throws Exception;

	public List<TypeMapping> getTypeMappings() {
		return typeMappings;
	}

	protected boolean checkMethodName(String methodName) {
		if (!methodName.startsWith(METHOD_PREFIX)) {
			return false;
		}
		if (!TapType.getNames().contains(StringUtils.removeStart(methodName, METHOD_PREFIX))) {
			return false;
		}
		return true;
	}

	protected TapType getTapType(String methodName) {
		if (!methodName.startsWith(METHOD_PREFIX)) {
			return null;
		}

		String tapTypeStr = StringUtils.removeStart(methodName, METHOD_PREFIX);
		if (TapType.getNames().contains(tapTypeStr)) {
			return TapType.valueOf(tapTypeStr);
		} else {
			return null;
		}
	}
}
