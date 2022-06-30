package com.tapdata.constant;

/**
 * 超时比率计算工具
 * <pre>
 * Author: <a href="mailto:harsen_lin@163.com">Harsen</a>
 * CreateTime: 2021/9/3 下午4:41
 * </pre>
 *
 * @param <T> 参数类型
 */
public interface IRateTimeout<T> {

	void exec(T args);

	/**
	 * 获取执行的超时比率
	 *
	 * @param timeout  超时时间
	 * @param callback 回调函数
	 * @param args     参数
	 * @param <T>      参数类型
	 * @return 超时比率
	 */
	static <T> float toRate(long timeout, IRateTimeout<T> callback, T args) {
		long startTimes = System.currentTimeMillis();
		callback.exec(args);
		return 1f * (System.currentTimeMillis() - startTimes) / timeout;
	}

	/**
	 * 计算下次批处理量
	 *
	 * @param timeout   超时时间
	 * @param batchSize 当前批处理量
	 * @param maxSize   最大批处理量
	 * @param minSize   最小批处理量
	 * @param timeRate  预设超时比率（越接近1越容易超时，最接近0效率越低）
	 * @param addRate   每次波动比率
	 * @param callback  回调函数
	 * @param args      参数
	 * @param <T>       参数类型
	 * @return 下次批处理量
	 */
	static <T> int toSize(long timeout, int batchSize, int maxSize, int minSize, float timeRate, float addRate, IRateTimeout<T> callback, T args) {
		// 执行率
		float resultRate = toRate(timeout, callback, args);
		resultRate = timeRate - resultRate;
		if (resultRate == 0) return batchSize;
		// 控制每次调控比例
		resultRate = Math.max(-addRate, Math.min(resultRate, addRate));
		// 控制最大最小值
		int resultSize = (int) (batchSize * (1 + resultRate));
		resultSize = Math.max(minSize, Math.min(resultSize, maxSize));
		return resultSize;
	}
}
