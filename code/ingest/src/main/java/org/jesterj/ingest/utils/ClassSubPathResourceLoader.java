package org.jesterj.ingest.utils;

import org.apache.lucene.analysis.util.ResourceLoader;

import java.io.IOException;
import java.io.InputStream;

public class ClassSubPathResourceLoader implements ResourceLoader {
  private final ClassLoader loader;
  private final String subpath;

  /**
   * Creates an instance using the given classloader to load Resources and classes.
   * Resource loading will be restricted to a portion of the classpath determined
   * by prepending the specified sub-path. Class find/load still uses the full classpath.
   *
   * @param loader the class loader to use
   * @param subpath the subpath within the classpath to load resources from
   */
  public ClassSubPathResourceLoader(ClassLoader loader, String subpath) {
    this.loader = loader;
    this.subpath = subpath;
  }

  @Override
  public InputStream openResource(String resource) throws IOException {
    String finalPath = subpath + resource;
    final InputStream stream = loader.getResourceAsStream(finalPath);
    if (stream == null)
      throw new IOException("Resource not found: " + resource);
    return stream;
  }

  @Override
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType) {
    try {
      return Class.forName(cname, true, loader).asSubclass(expectedType);
    } catch (Exception e) {
      throw new RuntimeException("Cannot load class: " + cname, e);
    }
  }

  @Override
  public <T> T newInstance(String cname, Class<T> expectedType) {
    Class<? extends T> clazz = findClass(cname, expectedType);
    try {
      return clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Cannot create instance: " + cname, e);
    }
  }
}
