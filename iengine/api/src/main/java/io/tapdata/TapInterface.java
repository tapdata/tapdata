package io.tapdata;

import com.tapdata.entity.Connections;
import io.tapdata.entity.BaseConnectionValidateResult;
import io.tapdata.entity.BaseConnectionValidateResultDetail;
import io.tapdata.entity.ConnectionsType;
import io.tapdata.entity.LoadSchemaResult;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public interface TapInterface {

	/**
	 * get supported functions
	 *
	 * @param supports
	 */
	Map<String, Boolean> getSupported(String[] supports);

	/**
	 * before test connections, init test items
	 *
	 * @return
	 */
	List<BaseConnectionValidateResultDetail> connectionsInit(ConnectionsType connectionsType);

	/**
	 * test connections by test items
	 *
	 * @param connections
	 * @return
	 */
	BaseConnectionValidateResult testConnections(Connections connections);

	/**
	 * load schema by connections
	 *
	 * @param connections
	 * @return
	 */
	LoadSchemaResult loadSchema(Connections connections);

	default String getUniqueName(Connections connections) throws Exception {
		if (connections == null || StringUtils.isBlank(connections.getId())) {
			return "";
		}
		return connections.getId();
	}
}
