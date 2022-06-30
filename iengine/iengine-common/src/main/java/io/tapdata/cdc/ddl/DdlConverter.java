package io.tapdata.cdc.ddl;

import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * DDL转换器
 *
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/12/10 下午10:26 Create
 * @since JDK1.8
 */
public interface DdlConverter<I extends DdlEvent, O> {

	/**
	 * 转换并输出
	 *
	 * @param in          输入
	 * @param outConsumer 输出处理器
	 */
	void convertDDL(I in, Consumer<O> outConsumer);

	/**
	 * 转换并输出
	 *
	 * @param in            输入
	 * @param outConsumer   输出处理器
	 * @param errorFunction 异常处理器
	 * @return 异常状态
	 */
	default boolean convertDDL(I in, Consumer<O> outConsumer, BiFunction<I, Exception, Boolean> errorFunction) {
		try {
			convertDDL(in, outConsumer);
			return true;
		} catch (Exception e) {
			return errorFunction.apply(in, e);
		}
	}
}
