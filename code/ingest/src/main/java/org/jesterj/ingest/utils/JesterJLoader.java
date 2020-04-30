package org.jesterj.ingest.utils;

import com.needhamsoftware.unojar.JarClassLoader;
import org.apache.velocity.exception.ResourceNotFoundException;
import org.jetbrains.annotations.Nullable;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

public class JesterJLoader extends URLClassLoader {

  private final LoaderWrapper parentLoader;
  private List<LoaderWrapper> extLoaders = Collections.synchronizedList(new ArrayList<>());
  private static final Object notNull = new Object();
  // should be more efficient than complex keys because strings should be interned by the
  // compiler/JIT and therefore cause no garbage.
  private final ThreadLocal<HashMap<String, Object>> classesLoaded =
      ThreadLocal.withInitial(HashMap::new);
  private final ThreadLocal<HashMap<String, Object>> resourcesLoaded =
      ThreadLocal.withInitial(HashMap::new);
  private final ThreadLocal<HashMap<String, Object>> classesFound =
      ThreadLocal.withInitial(HashMap::new);
  private final ThreadLocal<HashMap<String, Object>> resourcesFound =
      ThreadLocal.withInitial(HashMap::new);
  private final ThreadLocal<HashMap<String, Object>> librariesFound =
      ThreadLocal.withInitial(HashMap::new);

  public JesterJLoader(URL[] urls, ClassLoader parent) {
    super(urls, parent);
    if (parent == this) {
      throw new IllegalArgumentException(parent + " is it's own Parent!");
    }
    this.parentLoader = new LoaderWrapper(parent);
  }

  public void addExtLoader(ClassLoader loader) {
    if (loader == this) {
      throw new IllegalArgumentException("ClassLoader can't be an extLoader for itself!!");
    }
    extLoaders.add(new LoaderWrapper(loader));
  }

  @Override
  public URL[] getURLs() {
    List<URL> tmp = new ArrayList<>();
    for (LoaderWrapper extLoader : extLoaders) {
      tmp.addAll(extLoader.getUrls());
    }
    return tmp.toArray(new URL[0]);
  }

  @Nullable
  @Override
  public URL getResource(String name) {
    if (resourcesLoaded.get().get(name) == notNull) {
      return parentLoader.getResource(name);
    }
    try {
      resourcesLoaded.get().put(name, notNull);
      URL url = null;
      for (LoaderWrapper extLoader : extLoaders) {
        URL url1 = extLoader.getResource(name);
        if (url1 != null) {
          url = url1;
          break;
        }
      }
      if (url != null) return url;
      return super.getResource(name);
    } finally {
      resourcesLoaded.get().remove(name);
    }
  }

  @Override
  protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
    if (classesLoaded.get().get(name) == notNull) {
      return parentLoader.loadClass(name, resolve);
    }
    try {
      classesLoaded.get().put(name, notNull);
      Class<?> aClass = null;
      for (LoaderWrapper extLoader : extLoaders) {
        Class<?> aClass1 = null;
        try {
          aClass1 = extLoader.loadClass(name);
        } catch (ClassNotFoundException e) {
          // ignore, check next one or ultimately delegate up
        }
        if (aClass1 != null) {
          aClass = aClass1;
          break;
        }
      }
      if (aClass != null) return aClass;
      return super.loadClass(name, resolve);
    } finally {
      classesLoaded.get().remove(name);
    }
  }

  // These find methods provide location if it's in this class but do not delegate upwards
  // delegation happens in load/get methods

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    if (classesFound.get().get(name) == notNull) {
      return parentLoader.findClass(name);
    }
    try {
      classesFound.get().put(name, notNull);
      for (LoaderWrapper extLoader : extLoaders) {
        Class<?> aClass = null;
        try {
          aClass = extLoader.findClass(name);
        } catch (ClassNotFoundException e) {
          // ignore, check next one or ultimately delegate up
        }
        if (aClass != null) {
          return aClass;
        }
      }
      return super.findClass(name);
    } finally {
      classesFound.get().remove(name);
    }
  }

  @Override
  public URL findResource(String name) {
    if (resourcesFound.get().get(name) == notNull) {
      return parentLoader.findResource(name);
    }
    try {
      resourcesFound.get().put(name, notNull);
      for (LoaderWrapper extLoader : extLoaders) {
        URL url;
        url = extLoader.findResource(name);
        if (url != null) {
          return url;
        }
      }
      return super.findResource(name);
    } finally {
      resourcesFound.get().remove(name);
    }
  }

  @Override
  protected String findLibrary(String libName) {
    if (librariesFound.get().get(libName) == notNull) {
      return parentLoader.findLibrary(libName);
    }
    try {
      librariesFound.get().put(libName, notNull);

      for (LoaderWrapper extLoader : extLoaders) {
        String result = invokeFindLibrary(libName, extLoader);
        if (result != null) {
          return result;
        }
      }
      String result = invokeFindLibrary(libName, parentLoader);
      if (result != null) {
        return result;
      }
      return super.findLibrary(libName);
    } finally {
      librariesFound.get().remove(libName);
    }
  }

  private String invokeFindLibrary(String libName, ClassLoader parent) {
    try {
      Method m = parent.getClass().getDeclaredMethod("findLibrary", String.class);
      return (String) m.invoke(this.parentLoader, libName);
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      e.printStackTrace();
    }
    return null;
  }

  private static class LoaderWrapper extends ClassLoader {

    private final ClassLoader parent;

    protected LoaderWrapper(ClassLoader parent) {
      super(parent);
      this.parent = parent;
    }

    public List<URL> getUrls() {
      List<URL> urls = new ArrayList<>();
      if (parent instanceof URLClassLoader) {
        Collections.addAll(urls, ((URLClassLoader) parent).getURLs());
        return urls;
      }
      if (parent instanceof JarClassLoader) {

        String jarPath = ((JarClassLoader) parent).getOneJarPath();

        try {
          String jarUrlStr = "jar:" + jarPath;
          urls.add(new URL(jarUrlStr + "!/lib"));
          urls.add(new URL(jarUrlStr + "!/main"));
        } catch (MalformedURLException e) {
          throw new RuntimeException("bad URL for UnoJar!", e);
        }
        return urls;
      }
      return urls;
    }

    @Override
    public Class<?> findClass(String name) throws ClassNotFoundException {
      return (Class<?>) callParentMethod("findClass", new Class[]{String.class}, name);
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      return (Class<?>) callParentMethod("loadClass", new Class[]{String.class, Boolean.TYPE}, name, resolve);
    }

    @Override
    protected URL findResource(String name) {
      try {
        return (URL) callParentMethod("findResource", new Class[]{String.class}, name);
      } catch (ClassNotFoundException e) {
        // should not be possible
        throw new ResourceNotFoundException(e.getMessage());
      }
    }

    @Override
    public String findLibrary(String libName) {
      try {
        return (String) callParentMethod("findLibrary", new Class[]{String.class}, libName);
      } catch (ClassNotFoundException e) {
        // should not be possible
        throw new RuntimeException(e.getMessage());
      }
    }

    @SuppressWarnings("rawtypes")
    private Object callParentMethod(String methodName, Class[] types, Object... args) throws ClassNotFoundException {
      NoSuchMethodException originalException = null;
      Class clazz = parent.getClass();
      do {
        try {
          @SuppressWarnings("unchecked")
          Method m = clazz.getDeclaredMethod(methodName, types);
          m.setAccessible(true);
          return m.invoke(parent, args);
        } catch (NoSuchMethodException e) {
          if (originalException == null) {
            originalException = e;
          }
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
          Throwable cause = e.getCause();
          if (cause instanceof ClassNotFoundException) {
            throw (ClassNotFoundException) cause;
          }
          throw new RuntimeException(e);
        }
      } while ( (clazz = clazz.getSuperclass()) != null);
      throw new RuntimeException(originalException);
    }
  }
}
