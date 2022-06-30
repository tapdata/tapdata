package io.tapdata.pdk.core.classloader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.jar.JarFile;

/*
优先检查子，再检查父
 */
public class DependencyURLClassLoader extends ClassLoader {
    private static final String TAG = DependencyURLClassLoader.class.getSimpleName();

    private ChildURLClassLoader childClassLoader;

    public void close() {
        if(childClassLoader != null) {
            try {
                Class clazz = java.net.URLClassLoader.class;
                Field ucp = clazz.getDeclaredField("ucp");
                ucp.setAccessible(true);
                Object sunMiscURLClassPath = ucp.get(childClassLoader);
                Field loaders = sunMiscURLClassPath.getClass().getDeclaredField("loaders");
                loaders.setAccessible(true);
                Object collection = loaders.get(sunMiscURLClassPath);
                for (Object sunMiscURLClassPathJarLoader : ((Collection) collection).toArray()) {
                    try {
                        Field loader = sunMiscURLClassPathJarLoader.getClass().getDeclaredField("jar");
                        loader.setAccessible(true);
                        Object jarFile = loader.get(sunMiscURLClassPathJarLoader);
                        JarFile theJarFile = ((JarFile) jarFile);
                        TapLogger.info(TAG, "Closing jar file {}", theJarFile.getName());
                        theJarFile.close();
                        CommonUtils.ignoreAnyError(() -> {
                            FileUtils.forceDelete(new File(theJarFile.getName()));
                            TapLogger.info(TAG, "Deleted jar file {}", theJarFile.getName());
                        }, TAG);
                    } catch (Throwable t) {
                        // if we got this far, this is probably not a JAR loader so skip it
                        TapLogger.error(TAG, "Closing jar file failed, error {}", t.getMessage());
                    }
                }
            } catch (Throwable t) {
                // probably not a SUN VM
                TapLogger.error(TAG, "Closing jar file failed, error {}", t.getMessage());
            }
            try {
                childClassLoader.close();
                TapLogger.info(TAG, "Closing classloader {} urls {}", childClassLoader.hashCode(), Arrays.toString(childClassLoader.getURLs()));
            } catch (Throwable e) {
//                e.printStackTrace();
                TapLogger.error(TAG, "Closing classloader failed, error {}", e.getMessage());
            }
        }
    }

    /**
     * This class allows me to call findClass on a classloader
     */
    private static class FindClassClassLoader extends ClassLoader
    {
        public FindClassClassLoader(ClassLoader parent)
        {
            super(parent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException
        {
            return super.findClass(name);
        }
    }

    /**
     * This class delegates (child then parent) for the findClass method for a URLClassLoader.
     * We need this because findClass is protected in URLClassLoader
     */
    private static class ChildURLClassLoader extends URLClassLoader {
        private FindClassClassLoader realParent;

        ChildURLClassLoader(URL[] urls, FindClassClassLoader realParent) {
            super(urls, null);

            this.realParent = realParent;
//            TapLogger.info(TAG, "ChildURLClassLoader created with urls {} parent class loader {}", Arrays.toString(urls), realParent);
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
//            TapLogger.info(TAG, "Find class {}", name);
            Class<?> loaded = super.findLoadedClass(name);
            if( loaded != null ) {
//                TapLogger.info(TAG, "Found class {} hash {} for name {} in loaded classes", loaded, loaded.hashCode(), name);
                return loaded;
            }
            try {
                // first try to use the URLClassLoader findClass
                Class<?> clazz = super.findClass(name);
//                TapLogger.info(TAG, "Found class {} hash {} for name {} in PDK jar", clazz, clazz.hashCode(), name);
                return clazz;
            }
            catch( ClassNotFoundException e ) {
                // if that fails, we ask our real parent classloader to load the class (we give up)
                Class<?> clazz = realParent.loadClass(name);
//                TapLogger.info(TAG, "Found class {} hash {} for name {} in parent", clazz, clazz.hashCode(), name);
                return clazz;
            } catch (LinkageError linkageError) {
                TapLogger.info(TAG, "Occurred linkage error for name {}, {}", name, linkageError.getMessage());
                throw linkageError;
            }
        }
    }

    public DependencyURLClassLoader(Collection<URL> urls) {
        if(urls == null || urls.isEmpty()) {
            throw new IllegalArgumentException("urls is empty while initiate DependencyURLClassLoader");
        }
        URL[] urlArray = new URL[urls.size()];
        urls.toArray(urlArray);

        childClassLoader = new ChildURLClassLoader(urlArray, new FindClassClassLoader(DependencyURLClassLoader.class.getClassLoader()) );
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
        return childClassLoader.findClass(name);
    }
    @Override
    public URL findResource(final String name) {
        return childClassLoader.findResource(name);
    }

    @Override
    public Enumeration<URL> findResources(final String name) throws IOException {
      return childClassLoader.getResources(name);
    }
    @Override
    protected synchronized Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        try {
            // first we try to find a class inside the child classloader
            return childClassLoader.findClass(name);
        }
        catch( ClassNotFoundException e ) {
            // didn't find it, try the parent
//            TapLogger.info(TAG, "Class not found for name {} resolve {}", name, resolve);
            return super.loadClass(name, resolve);
        } catch (Throwable t) {
//            TapLogger.info(TAG, "Unknown error for name {} resolve {} error {}", name, resolve, t.getMessage());
            throw t;
        }
    }

    public ClassLoader getActualClassLoader() {
      return childClassLoader;
    }
}
