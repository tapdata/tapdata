package com.tapdata.tm.module.dto;

import java.util.List;

/**
 * 给人<b>手工执行</b>的建索引语句（纯函数）。TAP-12057 · P4-1。
 *
 * <p>部署计划表回答「将建哪几条索引」，但审批人常常还要自己去目标库确认、或在大集合上先手工建一遍
 * 再放行。语句由 TM 生成而不是让人照着表格手拼——<b>手拼最容易拼错的正是方向</b>，而「声明降序、
 * 建成升序」就是本工单起因的那个缺陷（§2.4）。</p>
 *
 * <p>形态与前端「推荐索引」保持一致（{@code recommend.ts}）：一律带 {@code background: true}
 * ——语句是给人在**版本未知**的目标库上执行的，MongoDB 4.2+ 忽略它、4.0 及更早需要它（否则前台
 * 建索引会阻塞该库读写），带上它在新版是空操作、在老版是保护。</p>
 *
 * <p><b>字段名一律加引号</b>：{@code POLICY.POLICY_STATUS} 这种点号路径不加引号就是非法 JS。
 * （前端那份生成器目前不加引号，点号路径会产出无法执行的语句——已记 TODO。）</p>
 *
 * <p>只有 MongoDB 出语句。其它数据源的建索引语法各不相同，凭空生成一条<b>可能跑不通甚至跑错</b>的
 * 语句，比不给更糟。</p>
 */
public final class ServingIndexManualCommands {

	private static final String MONGODB = "mongodb";

	private ServingIndexManualCommands() {
	}

	/**
	 * 按连接的数据源类型出语句。
	 *
	 * @param databaseType 连接的 {@code database_type}（如 {@code MongoDB}），大小写不敏感
	 * @return 非 MongoDB、或无法生成时返回 {@code null}
	 */
	public static String of(String databaseType, String table, ServingIndex index) {
		return databaseType != null && MONGODB.equalsIgnoreCase(databaseType.trim())
				? mongo(table, index)
				: null;
	}

	/**
	 * MongoDB 的 {@code createIndex} 语句。
	 *
	 * <p>字段或表名缺失时返回 {@code null}——建不出来的东西不能给人去执行。索引名缺失时不写
	 * {@code name}，让 MongoDB 自己按键取名，而不是凭空编一个（编出来的名字与平台后续比对
	 * 无关——身份是有序字段 + 方向，名字不参与——但给人看的语句不该出现平台自造的假名）。</p>
	 */
	public static String mongo(String table, ServingIndex index) {
		if (index == null || isBlank(table)) {
			return null;
		}
		List<ServingIndexField> fields = index.getFields();
		if (fields == null || fields.isEmpty()) {
			return null;
		}
		StringBuilder keys = new StringBuilder();
		for (ServingIndexField field : fields) {
			if (field == null || isBlank(field.getField())) {
				return null;
			}
			if (keys.length() > 0) {
				keys.append(", ");
			}
			// 方向与身份签名同一口径：只有显式 false 才是 -1（null 按升序，见 ServingIndexSignature）。
			keys.append('"').append(field.getField().trim()).append("\": ")
					.append(Boolean.FALSE.equals(field.getAsc()) ? "-1" : "1");
		}

		StringBuilder options = new StringBuilder();
		if (!isBlank(index.getName())) {
			options.append("name: \"").append(index.getName().trim()).append("\", ");
		}
		if (Boolean.TRUE.equals(index.getUnique())) {
			options.append("unique: true, ");
		}
		options.append("background: true");

		return "db." + table.trim() + ".createIndex({ " + keys + " }, { " + options + " })";
	}

	private static boolean isBlank(String value) {
		return value == null || value.trim().isEmpty();
	}
}
