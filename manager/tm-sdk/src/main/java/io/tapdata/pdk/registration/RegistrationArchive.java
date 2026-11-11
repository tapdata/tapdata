package io.tapdata.pdk.registration;

import io.tapdata.pdk.core.classloader.DependencyURLClassLoader;
import io.tapdata.pdk.core.classloader.ExternalJarManager;
import io.tapdata.pdk.core.tapnode.TapNodeClassFactory;
import io.tapdata.pdk.core.tapnode.TapNodeInfo;

import java.io.File;
import java.util.Collection;

/** A request-scoped use of the same PDK jar loader used by TapConnectorManager. */
public final class RegistrationArchive implements AutoCloseable {
    private final TapNodeClassFactory factory = new TapNodeClassFactory();
    private DependencyURLClassLoader classLoader;
    private Throwable loadError;

    public Collection<TapNodeInfo> load(File jar) {
        ExternalJarManager.build()
                .withJarFoundListener((file, firstTime) -> true)
                .withJarAnnotationHandlersListener(file -> factory.getClassAnnotationHandlers())
                .withJarLoadCompletedListener((file, loader, error) -> {
                    classLoader = (DependencyURLClassLoader) loader;
                    loadError = error;
                })
                .loadJars(jar.getAbsolutePath());
        if (loadError != null) {
            throw new IllegalStateException("Unable to load connector " + jar.getName(), loadError);
        }
        factory.applyNewClassloader(classLoader);
        return factory.getConnectorTapNodeInfos();
    }

    @Override
    public void close() {
        if (classLoader != null) {
            classLoader.close();
        }
    }
}
