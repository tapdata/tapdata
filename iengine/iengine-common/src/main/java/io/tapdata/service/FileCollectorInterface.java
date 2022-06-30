package io.tapdata.service;

import java.io.InputStream;
import java.util.Map;

/**
 * @author samuel
 */
public interface FileCollectorInterface {

	void init(String host, Integer port, String username, String password, String account, Map<String, Object> config);

	boolean connect() throws Exception;

	boolean disConnect() throws Exception;

	/**
	 * Get file list
	 *
	 * @param paths
	 * @param recursive
	 * @param includeFilenamePattern
	 * @param excludeFilenamePattern
	 * @param sort
	 * @param offsetTimestamp
	 * @param limitEveryPath         limit files for every path
	 * @return A Map<String, String> like {"a/t1.txt": "a", "a/b/t3.txt": "a/b"}
	 */
	Map<String, String> listFiles(String[] paths, Boolean[] recursive, String[] includeFilenamePattern,
								  String[] excludeFilenamePattern, String[] sort, String[] offsetTimestamp, int limitEveryPath) throws Exception;

	/**
	 * @param pathStr
	 * @return file can read: true;
	 * directory can read && execute: true;
	 * other conditions: false(not exists/wrong pathStr...)
	 * @throws Exception
	 */
	boolean isReadable(String pathStr) throws Exception;

	void completePendingCommand() throws Exception;

	boolean fileExists(String path, String filename) throws Exception;

	boolean deleteFile(String path, String filename) throws Exception;

	/**
	 * @param inputStream
	 * @param path
	 * @param filename
	 * @param length        file length
	 * @param fileOverwrite overwrite: if file exists, then delete old file and write new file
	 *                      discard: if file exists, discard this file
	 * @param bufferSize    stream write chunk size(Byte)
	 * @param writeMode     memory: read all file to memory then write
	 *                      stream: write file chunk by chunk
	 * @return
	 * @throws Exception
	 */
	Object writeFile(InputStream inputStream, String path, String filename, int length, String fileOverwrite, int bufferSize, String writeMode) throws Exception;

	default boolean check(String[] paths, Boolean[] recursive, String[] includeFilenamePattern,
						  String[] excludeFilenamePattern) {
		return paths != null && paths.length > 0
				&& includeFilenamePattern != null && includeFilenamePattern.length > 0
				&& excludeFilenamePattern != null && excludeFilenamePattern.length > 0
				&& recursive != null && recursive.length > 0
				&& paths.length == includeFilenamePattern.length
				&& paths.length == excludeFilenamePattern.length
				&& paths.length == recursive.length;
	}

	void stop() throws Exception;
}
