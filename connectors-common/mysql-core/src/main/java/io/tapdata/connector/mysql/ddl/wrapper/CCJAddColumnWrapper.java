package io.tapdata.connector.mysql.ddl.wrapper;

import io.tapdata.entity.event.ddl.TapDDLEvent;
import net.sf.jsqlparser.statement.Statement;

import java.util.function.Consumer;

/**
 * @author samuel
 * @Description
 * @create 2022-07-01 14:24
 **/
public class CCJAddColumnWrapper extends BaseDDLWrapper<Statement> {

	@Override
	public void wrap(Statement ddl, Consumer<TapDDLEvent> consumer) throws Throwable {

	}
}
