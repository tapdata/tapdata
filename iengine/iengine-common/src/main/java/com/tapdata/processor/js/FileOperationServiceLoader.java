package com.tapdata.processor.js;

import io.tapdata.file.operation.FileOperationErrorCode;
import io.tapdata.file.operation.FileOperationException;
import io.tapdata.file.operation.FileOperationServiceProvider;
import io.tapdata.file.operation.TapFileOperationService;

import java.util.Iterator;
import java.util.ServiceLoader;

/** Loads the shared connector implementation through the classloader boundary. */
public final class FileOperationServiceLoader {
    private FileOperationServiceLoader() {
    }

    public static TapFileOperationService load() {
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        if (loader == null) loader = FileOperationServiceLoader.class.getClassLoader();
        return load(loader);
    }

    static TapFileOperationService load(ClassLoader loader) {
        ServiceLoader<FileOperationServiceProvider> providers = ServiceLoader.load(
                FileOperationServiceProvider.class, loader);
        Iterator<FileOperationServiceProvider> iterator = providers.iterator();
        if (!iterator.hasNext()) {
            throw new FileOperationException(FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                    "shared file operation service provider is unavailable");
        }
        TapFileOperationService service = iterator.next().getService();
        if (service == null) {
            throw new FileOperationException(FileOperationErrorCode.FILE_SERVICE_UNAVAILABLE,
                    "shared file operation service provider returned null");
        }
        return service;
    }
}
