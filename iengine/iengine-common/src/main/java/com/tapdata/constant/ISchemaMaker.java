package com.tapdata.constant;

import com.tapdata.entity.Connections;
import com.tapdata.entity.RelateDataBaseTable;

import java.sql.SQLException;
import java.util.List;

/**
 * Created by tapdata on 16/11/2017.
 */
public interface ISchemaMaker {

	/**
	 * load oracle schema
	 *
	 * @param conn
	 * @return
	 * @throws SQLException
	 */
	List<RelateDataBaseTable> loadSchema(Connections conn) throws SQLException;

}
