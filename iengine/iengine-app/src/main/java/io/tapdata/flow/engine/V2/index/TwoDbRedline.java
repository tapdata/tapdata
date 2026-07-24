package io.tapdata.flow.engine.V2.index;

import com.mongodb.ConnectionString;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * P1-3 · 两库红线运行期断言（TAP-12057，见 ADR-0002）。
 *
 * <p>服务型索引本体只允许落在<b>用户库</b>（经引擎 PDK 连接器 + {@code Connections}）；绝不允许目标解析到
 * <b>平台自有库</b>（{@code TAPDATA_MONGO_URI}）。错库写入不报错（库存在、集合自动创建），此断言在读写前
 * 主动制造响亮失败。判定「是否同一目标库」= 有序主机集 + 库名（忽略 auth/options）。</p>
 */
public class TwoDbRedline {

	private TwoDbRedline() {
	}

	/**
	 * 断言索引操作的目标库 uri 不是平台自有库；命中即抛 {@link TwoDbRedlineViolationException}。
	 * 任一 uri 缺失或非法/非 mongo 时不误伤（放行）——不可能与平台 mongo 库相撞。
	 *
	 * @param targetUri   解析出的目标（用户库）连接 uri（{@code Connections.getDatabase_uri()}）
	 * @param platformUri 平台自有库 uri（{@code TAPDATA_MONGO_URI}）
	 */
	public static void assertTargetIsUserDb(String targetUri, String platformUri) {
		if (StringUtils.isBlank(targetUri) || StringUtils.isBlank(platformUri)) {
			return;
		}
		MongoIdentity target = MongoIdentity.parse(targetUri);
		MongoIdentity platform = MongoIdentity.parse(platformUri);
		if (target == null || platform == null) {
			return;
		}
		if (target.equals(platform)) {
			throw new TwoDbRedlineViolationException(
					"Two-DB redline violated: serving-index target resolved to the PLATFORM database (" + platform
							+ "), not a user database. Index body must be created only in the user DB via the PDK connector (ADR-0002).");
		}
	}

	/** mongo 连接身份 = 有序主机集 + 库名（忽略 auth/options），用于「是否同一目标库」判定。 */
	private static final class MongoIdentity {
		private final List<String> hosts;
		private final String database;

		private MongoIdentity(List<String> hosts, String database) {
			this.hosts = hosts;
			this.database = database;
		}

		static MongoIdentity parse(String uri) {
			try {
				ConnectionString cs = new ConnectionString(uri.trim());
				List<String> hosts = cs.getHosts().stream()
						.map(h -> h.toLowerCase().trim())
						.sorted()
						.collect(Collectors.toList());
				String db = cs.getDatabase() == null ? null : cs.getDatabase().toLowerCase();
				return new MongoIdentity(hosts, db);
			} catch (Exception e) {
				return null;
			}
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (!(o instanceof MongoIdentity)) return false;
			MongoIdentity that = (MongoIdentity) o;
			return Objects.equals(hosts, that.hosts) && Objects.equals(database, that.database);
		}

		@Override
		public int hashCode() {
			return Objects.hash(hosts, database);
		}

		@Override
		public String toString() {
			return String.join(",", hosts) + "/" + database;
		}
	}
}
