package test;

import io.tapdata.api.APIFactoryImpl;
import io.tapdata.entity.utils.InstanceFactory;
import io.tapdata.pdk.apis.api.APIFactory;
import org.junit.jupiter.api.Test;

/**
 * @author aplomb
 */
public class APIFactoryTest {
	@Test
	public void MainTest() {
		APIFactory apiFactory = InstanceFactory.instance(APIFactory.class);

	}
}
