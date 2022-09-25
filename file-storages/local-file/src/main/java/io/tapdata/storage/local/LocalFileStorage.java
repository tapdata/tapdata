package io.tapdata.storage.local;

import io.tapdata.file.TapFile;
import io.tapdata.file.TapFileStorage;
import io.tapdata.file.TapFileStorageBuilder;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static io.tapdata.entity.simplify.TapSimplify.entry;
import static io.tapdata.entity.simplify.TapSimplify.map;

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
	public void getFilesInDirectory(String directoryPath, String matchingReg, boolean recursive, int batchSize, Consumer<List<TapFile>> consumer) {

	}


	@Override
	public boolean isDirectoryExist(String path) {
		return false;
	}

}
