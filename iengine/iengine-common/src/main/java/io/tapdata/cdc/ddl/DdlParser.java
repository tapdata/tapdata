package io.tapdata.cdc.ddl;

import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * DDL解析器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/9/11 下午3:06 Create
 * @since JDK1.8
 */
public interface DdlParser<I, O extends DdlEvent> {

	/**
	 * 解析并输出
	 *
	 * @param in          输入
	 * @param outConsumer 输出处理器
	 */
	void parseDDL(I in, Consumer<O> outConsumer);

	/**
	 * 解析并输出
	 *
	 * @param in            输入
	 * @param outConsumer   输出处理器
	 * @param errorFunction 异常处理器
	 * @return 异常状态
	 */
	default boolean parseDDL(I in, Consumer<O> outConsumer, BiFunction<I, Exception, Boolean> errorFunction) {
		try {
			parseDDL(in, outConsumer);
			return true;
		} catch (Exception e) {
			return errorFunction.apply(in, e);
		}
	}
}
