package io.tapdata.inspect.cdc.operator;

import io.tapdata.inspect.cdc.IInspectCdcOperator;

/**
 * 增量校验操作抽象类
 *
 * @param <T> 偏移量类型
 * @author <a href="mailto:harsen_lin@163.com">Harsen</a>
 * @version v1.0 2021/11/3 下午9:05 Create
 */
public abstract class AbsInspectCdcOperator implements IInspectCdcOperator {
	private boolean isSource;

	public AbsInspectCdcOperator(boolean isSource) {
		this.isSource = isSource;
	}

	@Override
	public boolean isSource() {
		return isSource;
	}
}
