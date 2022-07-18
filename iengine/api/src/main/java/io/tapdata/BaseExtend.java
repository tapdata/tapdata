package io.tapdata;

import com.tapdata.entity.Connections;
import com.tapdata.entity.Mapping;

/**
 * @author samuel
 * @Description
 * @create 2021-07-07 15:59
 **/
public interface BaseExtend {

	/**
	 * count
	 *
	 * @param objectName
	 * @param connections
	 * @return
	 */
	default Long count(String objectName, Connections connections) {
		return 0L;
	}

	default Long count(String objectName, Connections connections, Mapping mapping) {
		return 0L;
	}
}
