package io.tapdata.mongodb.merge;

import java.util.List;

/**
 * The target supports merge write interfaces
 */
public interface Merge<TRecord, TResult> {

		/**
		 * append merge
		 * @param record
		 * @return
		 */
		default TResult appendMerge(TRecord record) {
				throw new UnsupportedOperationException("appendMerge does not implement.");
		}

		/**
		 * upsert merge
		 * @param record
		 * @return
		 */
		default TResult upsertMerge(TRecord record){
				throw new UnsupportedOperationException("upsertMerge does not implement.");
		}
		/**
		 * update merge
		 * @param record
		 * @return
		 */
		default TResult updateMerge(TRecord record) {
				throw new UnsupportedOperationException("updateMerge does not implement.");
		}

		/**
		 * update into array merge
		 * @param record
		 * @return
		 */
		default TResult updateIntoArrayMerge(TRecord record){
				throw new UnsupportedOperationException("updateIntoArrayMerge does not implement.");
		}

}