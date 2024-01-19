package base;

import com.fasterxml.jackson.core.type.TypeReference;
import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.constant.JSONUtil;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.MockTaskUtil;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.net.URL;

import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2023-11-16 11:10
 **/
public abstract class BaseTest {

	protected static ObsLogger mockObsLogger;
	protected static ClientMongoOperator mockClientMongoOperator;
	protected static SettingService mockSettingService;
	protected static ConfigurationCenter mockConfigurationCenter;

	@BeforeAll
	static void setup() {
		CommonUtils.setProperty(ConnectorConstant.JUNIT_TEST_PROP_KEY, "true");
		mockObsLogger = mock(ObsLogger.class);
		mockClientMongoOperator = mock(ClientMongoOperator.class);
		mockSettingService = mock(SettingService.class);
		mockConfigurationCenter = mock(ConfigurationCenter.class);
	}

	protected <T> T json2Pojo(String pathInResources, Class<T> toClazz) {
		URL taskJsonFileURL = MockTaskUtil.class.getClassLoader().getResource(pathInResources);
		if (null == taskJsonFileURL) {
			throw new RuntimeException(String.format("Cannot get url: '%s', check your json file name and path is correct", pathInResources));
		}
		T t;
		try {
			t = JSONUtil.json2POJO(taskJsonFileURL, toClazz);
		} catch (IOException e) {
			throw new RuntimeException("Parse json string to [" + toClazz.getName() + "] failed, url: " + taskJsonFileURL, e);
		}
		return t;
	}

	protected <T> T json2Pojo(String pathInResources, TypeReference<T> typeReference) {
		URL taskJsonFileURL = MockTaskUtil.class.getClassLoader().getResource(pathInResources);
		if (null == taskJsonFileURL) {
			throw new RuntimeException(String.format("Cannot get url: '%s', check your json file name and path is correct", pathInResources));
		}
		T t;
		try {
			t = JSONUtil.json2POJO(taskJsonFileURL, typeReference);
		} catch (IOException e) {
			throw new RuntimeException("Parse json string to [" + typeReference.getType().getTypeName() + "] failed, url: " + taskJsonFileURL, e);
		}
		return t;
	}
}
