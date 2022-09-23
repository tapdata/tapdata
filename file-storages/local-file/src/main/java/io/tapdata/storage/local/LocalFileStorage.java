package io.tapdata.storage.local;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

public class LocalFileStorage implements TapFileStorage {
	@Override
	public void init(Map<String, Object> params) {
		
	}

	@Override
	public void destroy() {

	}

	@Override
	public TapFile getFile(String path) {
		return null;
	}

	@Override
	public InputStream readFile(String path) {
		return null;
	}

	@Override
	public boolean isFileExist(String path) {
		return false;
	}

	@Override
	public boolean move(String sourcePath, String destPath) {
		return false;
	}

	@Override
	public boolean delete(String path) {
		return false;
	}

	@Override
	public TapFile saveFile(String path, InputStream is, boolean canReplace) {
		return null;
	}

	@Override
	public List<TapFile> getFilesInDirectory(String directoryPath, List<String> matchingReg, boolean recursive) {
		return null;
	}

	@Override
	public boolean isDirectoryExist(String path) {
		return false;
	}
}
