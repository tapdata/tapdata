package io.tapdata.ddlp.converters;

import io.tapdata.ddlp.DDLEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * DDL转换器 - 接口
 *
 * @param <I> 输入类型
 * @param <O> 输出类型
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/16 上午3:27 Create
 */
public interface DDLConverter<I extends DDLEvent, O> {

	/**
	 * 事件转 DDL
	 *
	 * @param in  事件
	 * @param out DDL消费
	 */
	void event2ddl(I in, Consumer<O> out);

	/**
	 * 事件转 DDL
	 *
	 * @param in 事件
	 * @return DDL
	 */
	default List<O> event2ddl(I in) {
		List<O> events = new ArrayList<>();
		event2ddl(in, events::add);
		return events;
	}

	/**
	 * 事件转 DDL
	 *
	 * @param in    输入
	 * @param out   输出处理器
	 * @param errFn 异常处理器
	 * @return 是否异常
	 */
	default boolean event2ddl(I in, Consumer<O> out, BiFunction<I, Exception, Boolean> errFn) {
		try {
			event2ddl(in, out);
			return true;
		} catch (Exception e) {
			return errFn.apply(in, e);
		}
	}
}
