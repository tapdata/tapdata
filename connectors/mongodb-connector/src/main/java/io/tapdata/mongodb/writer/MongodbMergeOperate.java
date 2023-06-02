package io.tapdata.mongodb.writer;

import com.mongodb.client.model.*;
import io.tapdata.entity.event.dml.TapDeleteRecordEvent;
import io.tapdata.entity.event.dml.TapInsertRecordEvent;
import io.tapdata.entity.event.dml.TapRecordEvent;
import io.tapdata.entity.event.dml.TapUpdateRecordEvent;
import io.tapdata.entity.schema.TapTable;
import io.tapdata.kit.EmptyKit;
import io.tapdata.mongodb.entity.MergeBundle;
import io.tapdata.mongodb.entity.MergeResult;
import io.tapdata.mongodb.util.MapUtil;
import io.tapdata.pdk.apis.entity.merge.MergeInfo;
import io.tapdata.pdk.apis.entity.merge.MergeLookupResult;
import io.tapdata.pdk.apis.entity.merge.MergeTableProperties;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author jackin
 * @date 2022/5/30 16:56
 **/
public class MongodbMergeOperate {

	public static List<WriteModel<Document>> merge(AtomicLong inserted, AtomicLong updated, AtomicLong deleted, TapRecordEvent tapRecordEvent, TapTable table) {
		List<WriteModel<Document>> writeModels = null;
		try {
			writeModels = new ArrayList<>();
			final MergeBundle mergeBundle = mergeBundle(tapRecordEvent);
			final Map<String, Object> info = tapRecordEvent.getInfo();
			if (tapRecordEvent instanceof TapInsertRecordEvent) {
				inserted.incrementAndGet();
			} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
				updated.incrementAndGet();
			} else if (tapRecordEvent instanceof TapDeleteRecordEvent) {
				deleted.incrementAndGet();
			}
			if (MapUtils.isNotEmpty(info) && info.containsKey(MergeInfo.EVENT_INFO_KEY)) {

				List<MergeResult> mergeResults = new ArrayList<>();
				final MergeInfo mergeInfo = (MergeInfo) info.get(MergeInfo.EVENT_INFO_KEY);
				final MergeTableProperties currentProperty = mergeInfo.getCurrentProperty();
				final List<MergeLookupResult> mergeLookupResults = mergeInfo.getMergeLookupResults();
				recursiveMerge(mergeBundle, currentProperty, mergeResults, mergeLookupResults, new MergeResult());

				if (CollectionUtils.isNotEmpty(mergeResults)) {
					for (MergeResult mergeResult : mergeResults) {
						final MergeResult.Operation operation = mergeResult.getOperation();
						switch (operation) {
							case INSERT:
								writeModels.add(new InsertOneModel<>(mergeResult.getInsert()));
								break;
							case UPDATE:
								writeModels.add(new UpdateManyModel<>(mergeResult.getFilter(), mergeResult.getUpdate(), mergeResult.getUpdateOptions()));
								break;
							case DELETE:
								writeModels.add(new DeleteOneModel<>(mergeResult.getFilter()));
								break;
						}
					}
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(String.format("tap event %s merge failed %s", tapRecordEvent, e.getMessage()), e);
		}
		return writeModels;
	}

	public static void recursiveMerge(
			MergeBundle mergeBundle,
			MergeTableProperties properties,
			List<MergeResult> mergeResults,
			List<MergeLookupResult> mergeLookupResults,
			MergeResult mergeResult
	) {

		switch (properties.getMergeType()) {
//						case appendWrite:
//								appendMerge(mergeBundle, properties, mergeResult);
//								break;
			case updateOrInsert:
				upsertMerge(mergeBundle, properties, mergeResult);
				break;
			case updateWrite:
				if (mergeResult.getOperation() != null) {
					mergeResults.add(mergeResult);
					mergeResult = new MergeResult();
				}
				updateMerge(mergeBundle, properties, mergeResult);
				break;
			case updateIntoArray:
				if (mergeResult.getOperation() != null) {
					mergeResults.add(mergeResult);
					mergeResult = new MergeResult();
				}
				updateIntoArrayMerge(mergeBundle, properties, mergeResult);
				break;
		}

		if (CollectionUtils.isNotEmpty(mergeLookupResults)) {
			for (MergeLookupResult mergeLookupResult : mergeLookupResults) {
				final Map<String, Object> data = mergeLookupResult.getData();
				mergeBundle = new MergeBundle(MergeBundle.EventOperation.INSERT, null, data);
				recursiveMerge(mergeBundle, mergeLookupResult.getProperty(), mergeResults, mergeLookupResult.getMergeLookupResults(), mergeResult);
			}
		}

		if (mergeResult != null) {
			mergeResults.add(mergeResult);
		}
	}

	public static void appendMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult) {
		final String targetPath = currentProperty.getTargetPath();
		Document insertDoc = new Document();
		final MergeBundle.EventOperation operation = mergeBundle.getOperation();
		switch (operation) {
			case INSERT:
			case UPDATE:
				if (EmptyKit.isNotEmpty(targetPath)) {
					insertDoc.put(targetPath, mergeBundle.getAfter());
				} else {
					insertDoc.putAll(mergeBundle.getAfter());
				}
				break;
			default:
				return;
		}
		mergeResult.getInsert().putAll(insertDoc);
	}

	public static void upsertMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult) {
		final String targetPath = currentProperty.getTargetPath();
//				final MergeResult.MergeResultBuilder builder = MergeResult.MergeResultBuilder.builder();
		final MergeBundle.EventOperation operation = mergeBundle.getOperation();
		final Document filter = filter(
				MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
				currentProperty.getJoinKeys()
		);
		mergeResult.getFilter().putAll(filter);
		switch (operation) {
			case INSERT:
			case UPDATE:
				Map<String, Object> after = mergeBundle.getAfter();
				Document setOperateDoc = new Document();
				Map<String, Object> flatValue = new Document();
				MapUtil.recursiveFlatMap(after, flatValue, "");
				after = MapUtils.isNotEmpty(flatValue) ? flatValue : after;
				if (EmptyKit.isNotEmpty(targetPath)) {
					for (Map.Entry<String, Object> entry : after.entrySet()) {
						setOperateDoc.append(targetPath + "." + entry.getKey(), entry.getValue());
					}
				} else {
					setOperateDoc.putAll(after);
				}
				final Document update = mergeResult.getUpdate();
				if (update.containsKey("$set")) {
					update.get("$set", Document.class).putAll(setOperateDoc);
				} else {
					update.put("$set", setOperateDoc);
				}
				if (operation == MergeBundle.EventOperation.INSERT) {
					mergeResult.getUpdateOptions().upsert(true);
				}
				if (mergeResult.getOperation() == null) {
					mergeResult.setOperation(MergeResult.Operation.UPDATE);
				}
				break;
			case DELETE:
				if (mergeResult.getOperation() == null) {
					mergeResult.setOperation(MergeResult.Operation.DELETE);
				}
				break;
		}
	}

	public static void updateMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult) {
		final String targetPath = currentProperty.getTargetPath();
		final boolean array = currentProperty.getIsArray();
		final Document filter = filter(
				MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
				currentProperty.getJoinKeys()
		);
		mergeResult.getFilter().putAll(filter);
		if (array) {
			final List<Document> arrayFilter = arrayFilter(
					MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
					currentProperty.getJoinKeys(),
					currentProperty.getTargetPath(),
					currentProperty.getArrayPath()
			);
			mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
		}

		Map<String, Object> value = MapUtils.isNotEmpty(mergeBundle.getAfter()) ? mergeBundle.getAfter() : mergeBundle.getBefore();
		final MergeBundle.EventOperation operation = mergeBundle.getOperation();

		String updatePatch = targetPath;
		if (array) {
			if (targetPath.contains(".")) {
				final String targetPathFirst = targetPath.substring(0, targetPath.lastIndexOf("."));
				final String targetPathLast = targetPath.substring(targetPath.lastIndexOf(".") + 1);
				updatePatch = targetPathFirst + ".$[element1]." + targetPathLast;
			} else {
				updatePatch = targetPath + ".$[element1]";
			}
		}

		Document updateOpDoc = new Document();
		Map<String, Object> flatValue = new Document();
		MapUtil.recursiveFlatMap(value, flatValue, "");
		value = MapUtils.isNotEmpty(flatValue) ? flatValue : value;
		if (EmptyKit.isNotEmpty(updatePatch)) {
			for (Map.Entry<String, Object> entry : value.entrySet()) {
				updateOpDoc.append(updatePatch + "." + entry.getKey(), entry.getValue());
			}
		} else {
			updateOpDoc.putAll(value);
		}
		if (mergeResult.getOperation() == null) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		switch (operation) {
			case INSERT:
			case UPDATE:
				if (mergeResult.getUpdate().containsKey("$set")) {
					mergeResult.getUpdate().get("$set", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$set", updateOpDoc);
				}
				break;
			case DELETE:
				if (mergeResult.getUpdate().containsKey("$unset")) {
					mergeResult.getUpdate().get("$unset", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$unset", updateOpDoc);
				}
				break;
		}
	}

	public static void updateIntoArrayMerge(MergeBundle mergeBundle, MergeTableProperties currentProperty, MergeResult mergeResult) {
		final String targetPath = currentProperty.getTargetPath();
		final boolean array = currentProperty.getIsArray();
		final MergeBundle.EventOperation operation = mergeBundle.getOperation();
		final List<String> arrayKeys = currentProperty.getArrayKeys();
		if (array) {
			final List<Document> arrayFilter;
			if (operation == MergeBundle.EventOperation.UPDATE) {
				arrayFilter = arrayFilter(
						MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
						currentProperty.getJoinKeys(),
						arrayKeys,
						currentProperty.getArrayPath()
				);
			} else {
				arrayFilter = arrayFilter(
						MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
						currentProperty.getJoinKeys(),
						currentProperty.getTargetPath(),
						currentProperty.getArrayPath());
			}
			mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
		} else {
			final Document filter = filter(
					MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
					currentProperty.getJoinKeys()
			);
			mergeResult.getFilter().putAll(filter);

			if (operation == MergeBundle.EventOperation.UPDATE) {
				final List<Document> arrayFilter = arrayFilterForArrayMerge(
						MapUtils.isNotEmpty(mergeBundle.getBefore()) ? mergeBundle.getBefore() : mergeBundle.getAfter(),
						currentProperty.getArrayKeys(),
						currentProperty.getTargetPath(),
						currentProperty.getArrayPath()
				);
				mergeResult.getUpdateOptions().arrayFilters(arrayFilter);
			}
		}

		Map<String, Object> after = mergeBundle.getAfter();
		Document updateOpDoc = new Document();

		if (mergeResult.getOperation() == null) {
			mergeResult.setOperation(MergeResult.Operation.UPDATE);
		}
		switch (operation) {
			case INSERT:
				mergeResult.getFilter().append("$or", Optional.of(arrayKeys).map(keys -> {
					List<Document> orList = new ArrayList<>();
					for (String arrayKey : keys) {
						if (array && targetPath.split("\\.").length > 1) {
							for (Map<String, String> joinKey : currentProperty.getJoinKeys()) {
								mergeResult.getFilter().append(joinKey.get("target"), after.get(joinKey.get("source")));
							}
						}
						orList.add(new Document(targetPath + "." + arrayKey, new Document("$ne", after.get(arrayKey))));
					}
					return orList;
				}).get());

				if (array) {
					String[] paths = targetPath.split("\\.");
					if (paths.length > 1) {
						updateOpDoc.append(paths[0] + ".$[element1]." + paths[1], after);
					} else {
						updateOpDoc.append(paths[0] + ".$[element1]", after);
					}
				} else {
					updateOpDoc.append(targetPath, after);
				}
				if (mergeResult.getUpdate().containsKey("$addToSet")) {
					mergeResult.getUpdate().get("$addToSet", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$addToSet", updateOpDoc);
				}
				break;
			case UPDATE:
				for (Map.Entry<String, Object> entry : after.entrySet()) {
					if (array) {
						String[] paths = targetPath.split("\\.");
						if (paths.length > 1) {
							updateOpDoc.append(paths[0] + ".$[element1]." + paths[1] + ".$[element2]." + entry.getKey(), entry.getValue());
						} else {
							updateOpDoc.append(targetPath + ".$[element1]." + entry.getKey(), entry.getValue());
						}
					} else {
						updateOpDoc.append(targetPath + ".$[element1]." + entry.getKey(), entry.getValue());
					}
				}
				if (mergeResult.getUpdate().containsKey("$set")) {
					mergeResult.getUpdate().get("$set", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$set", updateOpDoc);
				}
				break;
			case DELETE:
				for (String arrayKey : arrayKeys) {
					updateOpDoc.append(arrayKey, MapUtil.getValueByKey(mergeBundle.getBefore(), arrayKey));
				}
				updateOpDoc = new Document(targetPath, updateOpDoc);
				if (mergeResult.getUpdate().containsKey("$pull")) {
					mergeResult.getUpdate().get("$pull", Document.class).putAll(updateOpDoc);
				} else {
					mergeResult.getUpdate().put("$pull", updateOpDoc);
				}
				break;
		}
	}

	private static MergeBundle mergeBundle(TapRecordEvent tapRecordEvent) {
		Map<String, Object> before = null;
		Map<String, Object> after = null;
		MergeBundle.EventOperation eventOperation = null;
		if (tapRecordEvent instanceof TapInsertRecordEvent) {
			after = ((TapInsertRecordEvent) tapRecordEvent).getAfter();
			eventOperation = MergeBundle.EventOperation.INSERT;
		} else if (tapRecordEvent instanceof TapUpdateRecordEvent) {
			before = ((TapUpdateRecordEvent) tapRecordEvent).getBefore();
			after = ((TapUpdateRecordEvent) tapRecordEvent).getAfter();
			eventOperation = MergeBundle.EventOperation.UPDATE;
		} else {
			before = ((TapDeleteRecordEvent) tapRecordEvent).getBefore();
			eventOperation = MergeBundle.EventOperation.DELETE;
		}

		return new MergeBundle(eventOperation, before, after);
	}

	private static Document filter(Map<String, Object> data, List<Map<String, String>> joinKeys) {
		Document document = new Document();
		for (Map<String, String> joinKey : joinKeys) {
			document.put(joinKey.get("target"), MapUtil.getValueByKey(data, joinKey.get("source")));
		}
		return document;
	}

	private static List<Document> arrayFilter(Map<String, Object> data, List<Map<String, String>> joinKeys, String targetPath, String arrayPath) {
		List<Document> arrayFilter = new ArrayList<>();
		Document filter = new Document();
		for (Map<String, String> joinKey : joinKeys) {
//			String[] paths = joinKey.get("target").split("\\.");
			filter.put("element1." + getArrayMatchString(arrayPath, joinKey)/*paths[paths.length - 1]*/, MapUtil.getValueByKey(data, joinKey.get("source")));
		}
		arrayFilter.add(filter);
		return arrayFilter;
	}

	private static String getArrayMatchString(String arrayPath, Map<String, String> joinKey) {
		String targetStr = joinKey.get("target");
		if(targetStr.startsWith(arrayPath)) {
			targetStr = targetStr.substring(arrayPath.length() + 1);
		}
		return targetStr;
	}

	private static List<Document> arrayFilterForArrayMerge(Map<String, Object> data, List<String> arrayKeys, String targetPath, String arrayPath) {
		List<Document> arrayFilter = new ArrayList<>();
		Document filter = new Document();
		for (String arrayKey : arrayKeys) {
			String[] paths = arrayKey.split("\\.");
			filter.put("element1." + paths[paths.length - 1], MapUtil.getValueByKey(data, arrayKey));
		}
		arrayFilter.add(filter);
		return arrayFilter;
	}

	private static List<Document> arrayFilter(Map<String, Object> data, List<Map<String, String>> joinKeys, List<String> arrayKeys, String arrayPath) {
		List<Document> arrayFilter = new ArrayList<>();
		for (Map<String, String> joinKey : joinKeys) {
			Document filter = new Document();
//			String[] paths = joinKey.get("target").split("\\.");
			filter.put("element1." + getArrayMatchString(arrayPath, joinKey)/*paths[paths.length - 1]*/, MapUtil.getValueByKey(data, joinKey.get("source")));
			arrayFilter.add(filter);
		}

		for (String arrayKey : arrayKeys) {
			Document filter = new Document();
			String[] paths = arrayKey.split("\\.");
			filter.put("element2." + paths[paths.length-1], MapUtil.getValueByKey(data, arrayKey));
			arrayFilter.add(filter);
		}
		return arrayFilter;
	}

	private static WriteModel<Document> mergeResultToWriteMode(MergeResult mergeResult) {
		final MergeResult.Operation operation = mergeResult.getOperation();
		switch (operation) {
			case INSERT:
				return new InsertOneModel<>(mergeResult.getInsert());
			case UPDATE:
				return new UpdateManyModel<>(mergeResult.getFilter(), mergeResult.getUpdate(), mergeResult.getUpdateOptions());
			case DELETE:
				return new DeleteOneModel<>(mergeResult.getFilter());
		}
		return null;
	}
}
