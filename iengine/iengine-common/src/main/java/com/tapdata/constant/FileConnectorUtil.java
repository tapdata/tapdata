package com.tapdata.constant;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FileConnectorUtil {

	private static final Set<String> SUPPORTED_FILE_TYPE = new HashSet<>();

	static {
		SUPPORTED_FILE_TYPE.add("xls");
		SUPPORTED_FILE_TYPE.add("xlsx");
		SUPPORTED_FILE_TYPE.add("csv");
	}

	public static boolean isSyncableFile(File file) {
		String fileExtension = FileUtil.getFileExtension(file.getName());
		return file.isFile() &&
				SUPPORTED_FILE_TYPE.contains(fileExtension) &&
				Files.isReadable(Paths.get(file.getAbsolutePath()));
	}

	public static File[] selectLatestFiles(File[] files) throws IOException {
		Map<String, File> fileVersion = new HashMap<>();

		for (File file : files) {
			String tableName = fileTableName(file);
			long currentVersion = fileVersion(file);
			if (!fileVersion.containsKey(tableName)) {
				fileVersion.put(tableName, file);
			} else {
				File cacheFile = fileVersion.get(tableName);
				long version = fileVersion(cacheFile);
				if (currentVersion > version) {
					fileVersion.put(tableName, file);
				}
			}
		}
		int index = 0;
		File[] latestFiles = new File[fileVersion.size()];
		for (File file : fileVersion.values()) {
			latestFiles[index++] = file;
		}
		return latestFiles;
	}

	public static boolean modifingCheck(File file, int checkInterval) throws IOException, InterruptedException {
		long oldTS = FileUtil.lastModifyTS(file);
		Thread.sleep(checkInterval);
		long newTS = FileUtil.lastModifyTS(file);

		return newTS > oldTS;
	}

	public static Map<String, List<File>> groupByTableName(File[] files) {
		Map<String, List<File>> tableFiles = new HashMap<>();

		for (File file : files) {
			String tableName = fileTableName(file);
			if (!tableFiles.containsKey(tableName)) {
				tableFiles.put(tableName, new ArrayList<>());
			}
			tableFiles.get(tableName).add(file);
		}

		for (List<File> list : tableFiles.values()) {
			sortByFileVersion(list);
		}
		return tableFiles;
	}

	public static Map<String, List<File>> filterGroupByTableName(File[] files, long version) throws IOException {
		Map<String, List<File>> tableFiles = new HashMap<>();

		for (File file : files) {
			String tableName = fileTableName(file);
			long currentVersion = fileVersion(file);
			if (currentVersion <= version) {
				continue;
			}
			if (!tableFiles.containsKey(tableName)) {
				tableFiles.put(tableName, new ArrayList<>());
			}
			tableFiles.get(tableName).add(file);
		}

		for (List<File> list : tableFiles.values()) {
			sortByFileVersion(list);
		}
		return tableFiles;
	}

	private static boolean isUnderWriting(File file) {
		RandomAccessFile raFile = null;
		try {
			raFile = new RandomAccessFile(file, "r");
			return false;
		} catch (Exception e) {
			System.out.println("Skipping file " + file.getName() + " the file is under writing");
		} finally {
			if (raFile != null) {
				try {
					raFile.close();
				} catch (IOException e) {
					System.out.println("Exception during closing file " + file.getName());
				}
			}
		}
		return true;
	}

	private static boolean isCompletelyWritten(File file) {
		RandomAccessFile stream = null;
		try {
			stream = new RandomAccessFile(file, "rw");
			return true;
		} catch (Exception e) {
			System.out.println("Skipping file " + file.getName() + " for this iteration due it's not completely written");
		} finally {
			if (stream != null) {
				try {
					stream.close();
				} catch (IOException e) {
					System.out.println("Exception during closing file " + file.getName());
				}
			}
		}
		return false;
	}


	private static void sortByFileVersion(List<File> files) {

		Collections.sort(files, (file1, file2) -> {

			long version1 = 0;
			long version2 = 0;
			try {
				version1 = fileVersion(file1);
				version2 = fileVersion(file2);
			} catch (IOException e) {
				// abort
			}


			return version1 > version2 ? -1 : 1;
		});

	}

	public static String fileTableName(File file) {
//        String name = file.getName();
//        String[] split = name.split("\\.")[0].split("_");
//        return split[0];

		return file.getName().split("\\.")[0];
	}

	public static long fileVersion(File file) throws IOException {
		String name = file.getName();
		String[] split = name.split("\\.")[0].split("_");
		if (split.length < 2) {
			return 0l;
		}

		BasicFileAttributes basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
//        String version = split[1];
		return basicFileAttributes.lastModifiedTime().toMillis();
	}

	public static void main(String[] args) throws IOException {
		File file = new File("/Users/jackin/excel/Stream-3.6.1A-2017-10.zip");
//        System.out.println(isUnderWriting(file));
//        System.out.println(isCompletelyWritten(file));

		try {
			// Get a file channel for the file
			FileChannel channel = new RandomAccessFile(file, "rw").getChannel();

			// Use the file channel to create a lock on the file.
			// This method blocks until it can retrieve the lock.
//            FileLock lock = channel.lock();
			FileLock lock = null;

        /*
           use channel.lock OR channel.tryLock();
        */

			// Try acquiring the lock without blocking. This method returns
			// null or throws an exception if the file is already locked.
			try {
				lock = channel.tryLock();
				System.out.println(lock.isShared());
			} catch (OverlappingFileLockException e) {
				// File is already locked in this thread or virtual machine
				System.out.println(false);
			}

			// Release the lock - if it is not null!
			if (lock != null) {
				lock.release();
			}

			// Close the file
			channel.close();
		} catch (Exception e) {
		}
	}
}
