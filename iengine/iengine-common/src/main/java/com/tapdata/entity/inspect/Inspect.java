package com.tapdata.entity.inspect;

import com.tapdata.entity.BaseEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/7 10:18 上午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
public class Inspect extends BaseEntity implements Serializable {

	private String flowId;
	private String name;
	private String status;
	private String mode;

	/**
	 * row_count,field
	 */
	private String inspectMethod;
	/**
	 * 差异结果模式：All,OnSourceExists
	 */
	private String inspectDifferenceMode;

	private InspectTiming timing;
	private InspectLimit limit;

	private List<InspectTask> tasks;
	private String customId;
	private String user_id;

	private String version; // 校验任务编辑时生成版本号，用于表示结果属于哪个版本
	private String byFirstCheckId; // 如果是差异校验，需要指定对应批次第一次校验结果编号

	// 如果是增量校验: inspectMethod == "cdcCount"
	private int browserTimezoneOffset; // 浏览器时区偏移量，+8 = -480(分钟)
	private String cdcBeginDate; // 事件开始时间，不能为空。指定从哪个源事件操作时间开始校验，样式：'yyyy-MM-dd HH:mm'
	private int cdcDuration; // 每次处理时长，不能小于5；单位：分钟

	private String inspectResultId;     // 重新校验时，指定 inspect result id

	/**
	 * Inspect mode
	 */
	public enum Mode {
		/**
		 * cron mode
		 */
		CRON("cron"),
		/**
		 * one time mode
		 */
		MANUAL("manual"),
		;

		private String value;

		Mode(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}

		private static Map<String, Mode> map = new HashMap<>();

		static {
			for (Mode value : Mode.values()) {
				map.put(value.getValue(), value);
			}
		}

		public static Mode fromValue(String value) {
			return map.get(value);
		}
	}
}
