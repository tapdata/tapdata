package io.tapdata.modules.api.storage;

/**
 * @author aplomb
 */
public interface TapStorageFactory {
	TapStorageFactory init(StorageOptions storageOptions);
	TapSequenceStorage getSequenceStorage(String id);
	TapSequenceStorage deleteSequenceStorage(String id);

	TapKVStorage deleteKVStorage(String id);
	TapKVStorage getKVStorage(String id);

	class StorageOptions {
		public static StorageOptions create() {
			return new StorageOptions();
		}
		private String rootPath;
		public StorageOptions rootPath(String rootPath) {
			this.rootPath = rootPath;
			return this;
		}
		private boolean disableJavaSerializable = false;
		public StorageOptions disableJavaSerializable(boolean disableJavaSerializable) {
			this.disableJavaSerializable = disableJavaSerializable;
			return this;
		}

		public String getRootPath() {
			return rootPath;
		}

		public void setRootPath(String rootPath) {
			this.rootPath = rootPath;
		}

		public boolean isDisableJavaSerializable() {
			return disableJavaSerializable;
		}

		public void setDisableJavaSerializable(boolean disableJavaSerializable) {
			this.disableJavaSerializable = disableJavaSerializable;
		}

		@Override
		public String toString() {
			return StorageOptions.class.getSimpleName() + ": rootPath=" + rootPath + "; disableJavaSerializable=" + disableJavaSerializable + ". ";
		}
	}
}
