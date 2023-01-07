package io.tapdata.storage.sequence;

import io.tapdata.entity.error.CoreException;
import io.tapdata.entity.utils.ObjectSerializable;
import io.tapdata.modules.api.storage.TapStorageFactory;
import io.tapdata.storage.errors.StorageErrors;
import io.tapdata.storage.factory.TapStorageFactoryImpl;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author aplomb
 */
public class SequenceIterator implements Iterator<Object>, Closeable {
	private DataInputStream dataInputStream;
	private InputStream fileInputStream;
	private CompressorInputStream compressorInputStream;

	private final ObjectSerializable objectSerializable;
	private Integer nextLength;
	private String id;
	private TapStorageFactory.StorageOptions storageOptions;
	private File dbFile;
	private ObjectSerializable.ToObjectOptions toObjectOptions;
	public SequenceIterator(String id, TapStorageFactory.StorageOptions storageOptions, File dbFile, ObjectSerializable objectSerializable, ClassLoader classLoader) {
		this.id = id;
		this.storageOptions = storageOptions;
		this.dbFile = dbFile;
		this.objectSerializable = objectSerializable;
		toObjectOptions = new ObjectSerializable.ToObjectOptions().classLoader(classLoader);
	}
	public void ensureInit() {
		if(fileInputStream == null && compressorInputStream == null && dataInputStream == null) {
			synchronized (this) {
				if(fileInputStream == null && compressorInputStream == null && dataInputStream == null) {
					try {
						fileInputStream = FileUtils.openInputStream(dbFile);
						compressorInputStream = TapStorageFactoryImpl.factory.createCompressorInputStream(CompressorStreamFactory.ZSTANDARD, fileInputStream);
						dataInputStream = new DataInputStream(compressorInputStream);
					} catch (Throwable e) {
						throw new CoreException(StorageErrors.OPEN_INPUT_STREAM_FAILED, e, "Open InputStream for id {} storageOptions {} failed, {}", id, storageOptions, e.getMessage());
					}
				}
			}
		}
	}

	@Override
	public boolean hasNext() {
		ensureInit();
		if(nextLength == null) {
			try {
				nextLength = dataInputStream.readInt();
			} catch (IOException e) {
				if(e instanceof EOFException) {
					nextLength = -1;
				} else {
					throw new CoreException(StorageErrors.NEXT_LENGTH_READ_FAILED, e, "Read length failed {}", e.getMessage());
				}
			}
		}
		return nextLength >= 0;
	}

	@Override
	public Object next() {
		if(nextLength == null && !hasNext()) {
			throw new NoSuchElementException();
		}
		byte[] data = new byte[nextLength];
		try {
			dataInputStream.readFully(data);
			nextLength = null;
		} catch (IOException e) {
			throw new CoreException(StorageErrors.NEXT_READ_FULLY_FAILED, e, "Sequence iterator next failed, nextLength {}, error {}", nextLength, e.getMessage());
		}
		return objectSerializable.toObject(data, toObjectOptions);
	}

	@Override
	public void close() {
		if(dataInputStream != null)
			IOUtils.closeQuietly(dataInputStream);
		if(compressorInputStream != null)
			IOUtils.closeQuietly(compressorInputStream);
		if(fileInputStream != null)
			IOUtils.closeQuietly(fileInputStream);
	}
}
