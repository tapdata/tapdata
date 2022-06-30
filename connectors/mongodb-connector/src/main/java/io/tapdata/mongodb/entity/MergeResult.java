package io.tapdata.mongodb.entity;

import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

/**
 * @author jackin
 * @date 2022/5/30 17:50
 **/
public class MergeResult {

		private Document filter = new Document();

		private Document update = new Document();;

		private UpdateOptions updateOptions = new UpdateOptions();

		private Document insert = new Document();;

		private Operation operation;

		public Operation getOperation() {
				return operation;
		}

		public Document getFilter() {
				return filter;
		}

		public Document getUpdate() {
				return update;
		}

		public UpdateOptions getUpdateOptions() {
				return updateOptions;
		}

		public Document getInsert() {
				return insert;
		}

		public void setFilter(Document filter) {
				this.filter = filter;
		}

		public void setUpdate(Document update) {
				this.update = update;
		}

		public void setUpdateOptions(UpdateOptions updateOptions) {
				this.updateOptions = updateOptions;
		}

		public void setInsert(Document insert) {
				this.insert = insert;
		}

		public void setOperation(Operation operation) {
				this.operation = operation;
		}

		public enum Operation{
				INSERT,
				UPDATE,
				DELETE
		}
}
