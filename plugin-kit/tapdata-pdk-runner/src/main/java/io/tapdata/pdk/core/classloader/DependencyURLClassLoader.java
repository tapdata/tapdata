package io.tapdata.pdk.core.classloader;

import io.tapdata.entity.logger.TapLogger;
import io.tapdata.pdk.core.utils.CommonUtils;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

/*
优先检查子，再检查父
 */
public class DependencyURLClassLoader extends ClassLoader {
    private static final String TAG = DependencyURLClassLoader.class.getSimpleName();

    private ChildURLClassLoader childClassLoader;

    private Collection<URL> jarUrls;

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
                        TapLogger.debug(TAG, "Closing jar file {}", theJarFile.getName());
                        theJarFile.close();
                        CommonUtils.ignoreAnyError(() -> {
                            FileUtils.forceDelete(new File(theJarFile.getName()));
                            TapLogger.debug(TAG, "Deleted jar file {} from classloader", theJarFile.getName());
                        }, TAG);
                    } catch (Throwable t) {
                        // if we got this far, this is probably not a JAR loader so skip it
                        TapLogger.error(TAG, "Closing jar file failed, error {}", t.getMessage());
                    }
                }
            } catch (Throwable t) {
                // probably not a SUN VM
                TapLogger.warn(TAG, "Closing jar file failed, error {}", t.getMessage());
            }
            try {
                childClassLoader.close();
                TapLogger.debug(TAG, "Closing classloader {} urls {}", childClassLoader.hashCode(), Arrays.toString(childClassLoader.getURLs()));
            } catch (Throwable e) {
//                e.printStackTrace();
                TapLogger.warn(TAG, "Closing classloader failed, error {}", e.getMessage());
            } finally {
                if(jarUrls != null) {
                    for (URL jarUrl : jarUrls) {
                        File file = new File(jarUrl.getFile());
                        if(file.exists()) {
                            FileUtils.deleteQuietly(file);
                            TapLogger.debug(TAG, "Deleted jar file {}", file.getName());
                        }
                    }
                }
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
        private Map<String, String> manifest;
        ChildURLClassLoader(URL[] urls, FindClassClassLoader realParent) {
            super(urls, null);

            this.realParent = realParent;
//            TapLogger.debug(TAG, "ChildURLClassLoader created with urls {} parent class loader {}", Arrays.toString(urls), realParent);

            try {
                this.manifest = new HashMap<>();
                Enumeration<URL> enumeration = this.getResources("META-INF/MANIFEST.MF");
                while(enumeration.hasMoreElements()) {
                    Manifest manifest = new Manifest(enumeration.nextElement().openStream());
                    Attributes attributes = manifest.getMainAttributes();
                    Set<Object> keys = attributes.keySet();
                    for(Object key : keys) {
                        this.manifest.put(key.toString(), attributes.get(key).toString());
                    }
                }
            } catch (Throwable throwable) {
                TapLogger.debug(TAG, "Read manifest for jar {} failed, {}", this.getURLs(), throwable.getMessage());
            }
        }

        @Override
        public Class<?> findClass(String name) throws ClassNotFoundException {
//            TapLogger.debug(TAG, "Find class {}", name);
            Class<?> loaded = super.findLoadedClass(name);
            if( loaded != null ) {
//                TapLogger.debug(TAG, "Found class {} hash {} for name {} in loaded classes", loaded, loaded.hashCode(), name);
                return loaded;
            }
            try {
                // first try to use the URLClassLoader findClass
                Class<?> clazz = super.findClass(name);
//                TapLogger.debug(TAG, "Found class {} hash {} for name {} in PDK jar", clazz, clazz.hashCode(), name);
                return clazz;
            }
            catch( ClassNotFoundException e ) {
                // if that fails, we ask our real parent classloader to load the class (we give up)
                Class<?> clazz = realParent.loadClass(name);
//                TapLogger.debug(TAG, "Found class {} hash {} for name {} in parent", clazz, clazz.hashCode(), name);
                return clazz;
            } catch (LinkageError linkageError) {
                TapLogger.debug(TAG, "Occurred linkage error for name {}, {}", name, linkageError.getMessage());
                throw linkageError;
            }
        }

        public Map<String, String> manifest() {
            return manifest;
        }
    }

    public DependencyURLClassLoader(Collection<URL> urls) {
        if(urls == null || urls.isEmpty()) {
            throw new IllegalArgumentException("urls is empty while initiate DependencyURLClassLoader");
        }
        URL[] urlArray = new URL[urls.size()];
        urls.toArray(urlArray);
        jarUrls = urls;

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
//            TapLogger.debug(TAG, "Class not found for name {} resolve {}", name, resolve);
            return super.loadClass(name, resolve);
        } catch (Throwable t) {
//            TapLogger.debug(TAG, "Unknown error for name {} resolve {} error {}", name, resolve, t.getMessage());
            throw t;
        }
    }

    public ClassLoader getActualClassLoader() {
      return childClassLoader;
    }

    public Map<String, String> manifest() {
        return childClassLoader != null ? childClassLoader.manifest : null;
    }

}
