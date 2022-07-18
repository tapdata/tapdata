package io.tapdata.Schedule;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientException;
import com.mongodb.MongoServerException;
import com.mongodb.MongoSocketException;
import com.mongodb.client.ListCollectionsIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.result.DeleteResult;
import com.tapdata.constant.MongodbUtil;
import com.tapdata.entity.Connections;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static com.mongodb.client.model.Projections.fields;
import static com.mongodb.client.model.Projections.include;

public class GridFSCleaner {

	private static final int RETRY_TIMES = 3;

	private static Logger logger = LogManager.getLogger(GridFSCleaner.class);

	public static ClearGridFSResult startClean(Connections connection) throws UnsupportedEncodingException {
		ClearGridFSResult result = new ClearGridFSResult();
		List<String> filesCollections = new ArrayList<>();
		MongoClient client = null;
		int retry = 0;
		while (retry < RETRY_TIMES) {
			try {
				client = MongodbUtil.createMongoClient(connection);
				MongoDatabase mongoDatabase = client.getDatabase(MongodbUtil.getDatabase(connection));
				ListCollectionsIterable<Document> collections = mongoDatabase.listCollections();

				for (Document collection : collections) {
					String collectionName = collection.getString("name");
					if (collectionName.endsWith(".files")) {
						String type = collection.getString("type");
						if ("collection".equals(type)) {
							String name = collection.getString("name");
							filesCollections.add(name);
						}
					}
				}
				result = clearGridFS(filesCollections, mongoDatabase);
				result.setSuccess(true);
				break;
			} catch (MongoSocketException | MongoServerException | MongoClientException e) {
				if (retry < RETRY_TIMES) {
					retry++;
					logger.warn("Clear _id {} name {} connection gridfs files failed {} , will retry after 3s, remain retry times {}",
							connection.getId(), connection.getName(), e.getMessage(), RETRY_TIMES - retry
					);
					try {
						Thread.sleep(3000);
					} catch (InterruptedException e1) {
						// abort
					}
				} else {
					String failReason = "Clear _id " + connection.getId() + " name " + connection.getName() + " file failed exceed retry times " + RETRY_TIMES + " then stopped clear.";
					result.setFailedReason(failReason);
					result.setException(e);
					break;
				}
			} finally {
				MongodbUtil.releaseConnection(client, null);
			}
		}
		return result;
	}

	private static long mongodbServerTimestamp(MongoDatabase mongoDatabase) {
		Document result = mongoDatabase.runCommand(new Document("isMaster", 1));
		Date localTime = result.getDate("localTime");
		return localTime != null ? localTime.getTime() : 0;
	}

	private static ClearGridFSResult clearGridFS(List<String> filesCollections, MongoDatabase mongoDatabase) {
		ClearGridFSResult result = new ClearGridFSResult();
		long serverTS = mongodbServerTimestamp(mongoDatabase);
		for (String filesCollection : filesCollections) {
			List<Document> clearFiles = new ArrayList<>();

			MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(filesCollection);

			MongoCursor<Document> cursor = mongoCollection.find(new Document("metadata.expired_unix_ts", new Document("$lte", serverTS)))
					.projection(fields(include("_id", "filename", "metadata.file_size_ondisk")))
					.iterator();
			while (cursor.hasNext()) {
				clearFiles.add(cursor.next());
			}
			ClearGridFSResult subsetResult = clearFilesAndChunks(mongoDatabase, filesCollection, clearFiles);
			result.addClearGridFSResult(subsetResult);
		}

		return result;
	}

	private static ClearGridFSResult clearFilesAndChunks(MongoDatabase mongoDatabase, String filesCollection, List<Document> clearFiles) {
		ClearGridFSResult result = new ClearGridFSResult();
		for (Document clearFile : clearFiles) {
			ObjectId fileId = clearFile.getObjectId("_id");
			String filename = clearFile.getString("filename");

			StringBuilder sb = new StringBuilder();
			String prefix = StringUtils.removeEnd(filesCollection, ".files");
			String chunksCollection = sb.append(prefix).append(".").append("chunks").toString();
			MongoCollection<Document> filesMongoCollection = mongoDatabase.getCollection(filesCollection);
			MongoCollection<Document> chunksMongoCollection = mongoDatabase.getCollection(chunksCollection);

			int retry = 0;
			while (retry < RETRY_TIMES) {
				try {
					DeleteResult deleteResult = chunksMongoCollection.deleteMany(new Document("files_id", fileId));
					result.addToTotalChunks(deleteResult.getDeletedCount());
					retry = 0;

					filesMongoCollection.deleteOne(new Document("_id", fileId));
					result.addToDeletedFiles(1);

					Document metaData = clearFile.get("metadata", Document.class);
					if (MapUtils.isNotEmpty(metaData)) {
						Long fileSizeOndisk = metaData.getLong("file_size_ondisk");
						result.addToTotalSize(fileSizeOndisk == null ? 0 : fileSizeOndisk);
					}
					break;
				} catch (MongoSocketException | MongoServerException | MongoClientException e) {
					if (retry < RETRY_TIMES) {
						retry++;
						logger.warn("Clear {} file happened error {} will retry after 3s, remain retry times {}.", filename, e.getMessage(), RETRY_TIMES - retry);
						try {
							Thread.sleep(3000);
						} catch (InterruptedException e1) {
							// abort
						}
					} else {
						result.addToFailedFiles(1);
						throw new RuntimeException("Clear " + filename + " file failed exceed retry times " + RETRY_TIMES + " then stopped clear.", e);
					}
				}
			}
		}

		return result;
	}

	static class ClearGridFSResult {
		private long deletedFiles;
		private long totalChunks;
		private long totalSize;
		private long failedFiles;
		private boolean success;
		private Exception exception;
		private String failedReason;

		public ClearGridFSResult() {
		}

		public void addClearGridFSResult(ClearGridFSResult result) {
			addToDeletedFiles(result.getDeletedFiles());
			addToTotalChunks(result.getTotalChunks());
			addToTotalSize(result.getTotalSize());
			addToFailedFiles(result.getFailedFiles());
		}

		public void addToDeletedFiles(long deletedFiles) {
			this.deletedFiles += deletedFiles;
		}

		public void addToTotalChunks(long totalChunks) {
			this.totalChunks += totalChunks;
		}

		public void addToTotalSize(long totalSize) {
			this.totalSize += totalSize;
		}

		public void addToFailedFiles(long failedFiles) {
			this.failedFiles += failedFiles;
		}

		public long getDeletedFiles() {
			return deletedFiles;
		}

		public long getTotalChunks() {
			return totalChunks;
		}

		public long getTotalSize() {
			return totalSize;
		}

		public long getFailedFiles() {
			return failedFiles;
		}

		public boolean isSuccess() {
			return success;
		}

		public void setSuccess(boolean success) {
			this.success = success;
		}

		public Exception getException() {
			return exception;
		}

		public void setException(Exception exception) {
			this.exception = exception;
		}

		public String getFailedReason() {
			return failedReason;
		}

		public void setFailedReason(String failedReason) {
			this.failedReason = failedReason;
		}

		@Override
		public String toString() {
			final StringBuilder sb = new StringBuilder();
			sb.append(" deletedFiles: ").append(deletedFiles);
			sb.append(", totalChunks: ").append(totalChunks);
			sb.append(", totalSize: ").append(totalSize);
			sb.append(", failedFiles: ").append(failedFiles);
			return sb.toString();
		}
	}
}
