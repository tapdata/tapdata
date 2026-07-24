package io.tapdata.flow.engine.V2.index;

/**
 * P1-3 · 两库红线违规（TAP-12057，见 ADR-0002）。
 *
 * <p>服务型索引的读写目标解析到了<b>平台自有库</b>（{@code TAPDATA_MONGO_URI}）而非用户库时抛出。
 * 错库写索引本身不报错（库存在、集合自动创建），故必须主动制造这个响亮失败。</p>
 */
public class TwoDbRedlineViolationException extends RuntimeException {
	public TwoDbRedlineViolationException(String message) {
		super(message);
	}
}
