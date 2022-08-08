package com.tapdata.entity.inspect;

import com.tapdata.entity.BaseEntity;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Map;

/**
 * @author lg<lirufei0808 @ gmail.com>
 * @date 2020/9/22 9:45 下午
 * @description
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = true)
public class InspectDetail extends BaseEntity implements Serializable {

	private String inspect_id;
	private String taskId;
	private String type;
	private Map<String, Object> source;
	private Map<String, Object> target;
	private String inspectResultId;

	/**
	 * 高级校验: 用户执行脚本后返回的错误信息
	 */
	private String message;
	private String user_id;
	private String customId;
	/*private List<Row> details;

	@Getter
	@Setter
	@ToString
	public static class Row {
		private Field source;
		private Field target;
		public Row source(Field source) {
			this.source = source;
			return this;
		}
		public Row target(Field target) {
			this.target = target;
			return this;
		}
	}

	@Getter
	@Setter
	@ToString
	public static class Field {
		private Object value;
		private String type;
		private String field;

		public Field value(Object value) {
			this.value = value;
			return this;
		}
		public Field type(String type) {
			this.type = type;
			return this;
		}
		public Field field(String field) {
			this.field = field;
			return this;
		}
	}*/
}
