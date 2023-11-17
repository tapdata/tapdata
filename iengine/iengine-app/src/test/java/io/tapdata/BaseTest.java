package io.tapdata;

import com.tapdata.constant.ConfigurationCenter;
import com.tapdata.constant.ConnectorConstant;
import com.tapdata.mongo.ClientMongoOperator;
import io.tapdata.common.SettingService;
import io.tapdata.observable.logging.ObsLogger;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.junit.jupiter.api.BeforeAll;

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
}
