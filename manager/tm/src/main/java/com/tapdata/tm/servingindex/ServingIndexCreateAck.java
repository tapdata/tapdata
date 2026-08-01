package com.tapdata.tm.servingindex;

import java.util.Collections;
import java.util.List;

/**
 * 建索引回执。TAP-12057 · P3-3（ADR-0012 D1）。
 *
 * <p>「发出去了」不等于「建成了」：连接器会把 errorCode 85/86 catch 后 continue（ADR-0005 红线），
 * 逐条成败只有引擎回执里有。超时与整体失败同样不能当成「建成了」——否则部署会报成功而索引没建。</p>
 */
public final class ServingIndexCreateAck {

	private final List<String> created;
	private final List<String> failed;
	private final String error;
	private final boolean timedOut;

	private ServingIndexCreateAck(List<String> created, List<String> failed, String error, boolean timedOut) {
		this.created = created;
		this.failed = failed;
		this.error = error;
		this.timedOut = timedOut;
	}

	public static ServingIndexCreateAck of(List<String> created, List<String> failed) {
		return new ServingIndexCreateAck(
				created == null ? Collections.emptyList() : created,
				failed == null ? Collections.emptyList() : failed, null, false);
	}

	public static ServingIndexCreateAck failed(String error) {
		return new ServingIndexCreateAck(Collections.emptyList(), Collections.emptyList(), error, false);
	}

	public static ServingIndexCreateAck timeout() {
		return new ServingIndexCreateAck(Collections.emptyList(), Collections.emptyList(), null, true);
	}

	/** 建成的索引名。 */
	public List<String> getCreated() {
		return created;
	}

	/** 逐条失败，形如 {@code "a_1: <原因>"}。 */
	public List<String> getFailed() {
		return failed;
	}

	/** 整体失败信息（引擎侧 handler 报错）；成功或超时为 {@code null}。 */
	public String getError() {
		return error;
	}

	public boolean isTimedOut() {
		return timedOut;
	}
}
