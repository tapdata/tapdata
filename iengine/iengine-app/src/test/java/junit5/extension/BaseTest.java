package junit5.extension;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import static org.mockito.Mockito.mock;

/**
 * @author samuel
 * @Description
 * @create 2023-11-22 10:30
 **/
public class BaseTest implements BeforeAllCallback {
	protected static ObsLogger mockObsLogger;
	protected static ClientMongoOperator mockClientMongoOperator;
	protected static SettingService mockSettingService;
	protected static ConfigurationCenter mockConfigurationCenter;

	@Override
	public void beforeAll(ExtensionContext context) {
		CommonUtils.setProperty(ConnectorConstant.JUNIT_TEST_PROP_KEY, "true");
		mockObsLogger = mock(ObsLogger.class);
		mockClientMongoOperator = mock(ClientMongoOperator.class);
		mockSettingService = mock(SettingService.class);
		mockConfigurationCenter = mock(ConfigurationCenter.class);
	}

	public static ObsLogger getMockObsLogger() {
		return mockObsLogger;
	}

	public static ClientMongoOperator getMockClientMongoOperator() {
		return mockClientMongoOperator;
	}

	public static SettingService getMockSettingService() {
		return mockSettingService;
	}

	public static ConfigurationCenter getMockConfigurationCenter() {
		return mockConfigurationCenter;
	}
}
