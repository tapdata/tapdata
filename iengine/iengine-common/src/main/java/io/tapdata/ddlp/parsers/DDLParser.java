package io.tapdata.ddlp.parsers;

import io.tapdata.ddlp.DDLEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * DDL解析器 - 接口
 *
 * @param <I> 输入类型
 * @param <O> 输出类型
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:27 Create
 */
public interface DDLParser<I, O extends DDLEvent> {

	/**
	 * DDL 转事件
	 *
	 * @param in  DDL
	 * @param out 事件消费
	 */
	void ddl2Event(I in, Consumer<O> out);

	/**
	 * DDL 转事件
	 *
	 * @param in DDL
	 * @return 事件
	 */
	default List<O> ddl2Event(I in) {
		List<O> events = new ArrayList<>();
		ddl2Event(in, events::add);
		return events;
	}

	/**
	 * DDL 转事件
	 *
	 * @param in    输入
	 * @param out   输出处理器
	 * @param errFn 异常处理器
	 * @return 是否异常
	 */
	default boolean ddl2Event(I in, Consumer<O> out, BiFunction<I, Exception, Boolean> errFn) {
		try {
			ddl2Event(in, out);
			return true;
		} catch (Exception e) {
			return errFn.apply(in, e);
		}
	}
}
