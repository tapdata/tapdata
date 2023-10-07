package io.tapdata.mongodb.entity;

import java.util.Map;

/**
 * @author jackin
 * @date 2022/5/30 17:39
 **/
public class MergeBundle {

		private EventOperation operation;

		private Map<String, Object> before;

		private Map<String, Object> after;

		private Map<String, Object> removefields;

		public MergeBundle(EventOperation operation, Map<String, Object> before, Map<String, Object> after) {
				this.operation = operation;
				this.before = before;
				this.after = after;
		}

	public MergeBundle(EventOperation operation, Map<String, Object> before, Map<String, Object> after, Map<String, Object> removefields) {
		this.operation = operation;
		this.before = before;
		this.after = after;
		this.removefields = removefields;
	}

	public Map<String, Object> getRemovefields() {
		return removefields;
	}

	public void setRemovefields(Map<String, Object> removefields) {
		this.removefields = removefields;
	}

	public EventOperation getOperation() {
				return operation;
		}

		public void setOperation(EventOperation operation) {
				this.operation = operation;
		}

		public Map<String, Object> getBefore() {
				return before;
		}

		public void setBefore(Map<String, Object> before) {
				this.before = before;
		}

		public Map<String, Object> getAfter() {
				return after;
		}

		public void setAfter(Map<String, Object> after) {
				this.after = after;
		}

		public enum EventOperation {
				INSERT,
				UPDATE,
				DELETE
		}
}
