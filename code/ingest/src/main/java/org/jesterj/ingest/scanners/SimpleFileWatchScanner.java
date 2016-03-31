/*
 * Copyright 2016 Needham Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.jesterj.ingest.scanners;

import com.sun.nio.file.SensitivityWatchEventModifier;
import net.jini.space.JavaSpace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 1/27/16
 */

/**
 * Scanner for local filesystems. This scanner operates in a two phase process. First a full walk of the filesystem
 * is performed, and then further changes are detected by a {@link WatchService}. No persistent
 * record of files detected is kept, so while this will only send files once and then only updated/modified
 * files during it's run, restarting will send all files in the directory again.
 */
public class SimpleFileWatchScanner extends ScannerImpl {
  private static final Logger log = LogManager.getLogger();

  private File rootDir;
  LinkedHashMap<File, WatchService> watchers = new LinkedHashMap<>();
  private final Object watcherLock = new Object();

  protected SimpleFileWatchScanner() {
  }

  @Override
  public Function<String, String> getIdFunction() {
    return s -> s;
  }

  @Override
  public Consumer<Document> getDocumentTracker() {
    return document -> {
    };
  }

  @Override
  public Runnable getScanOperation() {
    return () -> {
      // set up our watcher if needed
      synchronized (watcherLock) {
        if (watchers.size() == 0) {
          try {
            Files.walkFileTree(rootDir.toPath(), new RootWalker());
          } catch (IOException e) {
            log.error("failed to walk filesystem!", e);
            throw new RuntimeException(e);
          }
        }
      }
      // Process pending events
      for (File dir : watchers.keySet()) {
        WatchService watcher = watchers.get(dir);
        for (WatchKey key; (key = watcher.poll()) != null; ) {
          for (WatchEvent<?> event : key.pollEvents()) {
            if (OVERFLOW == event.kind()) {
              //TODO need to look into what causes this and how to avoid this case...
              log.error("too many simultaneous watch events. Some filesystem were lost!", key);
            }
            @SuppressWarnings("unchecked")
            WatchEvent<Path> fileEvent = (WatchEvent<Path>) event;
            Path resolvedPath = dir.toPath().resolve(fileEvent.context());
            if (resolvedPath.toFile().isDirectory()) {
              continue;
            }

            if (ENTRY_DELETE == fileEvent.kind()) {
              makeDoc(resolvedPath, Document.Operation.DELETE, null);
              continue;
            }

            BasicFileAttributeView view = Files.getFileAttributeView(resolvedPath, BasicFileAttributeView.class);
            BasicFileAttributes attrs = null;
            try {
              attrs = view.readAttributes();
            } catch (IOException e) {
              log.warn("Could not read attributes for file:{}", resolvedPath);
            }

            if (ENTRY_CREATE == fileEvent.kind()) {

              makeDoc(resolvedPath, Document.Operation.NEW, attrs);
            }
            if (ENTRY_MODIFY == fileEvent.kind()) {
              makeDoc(resolvedPath, Document.Operation.UPDATE, attrs);
            }
          }
          key.reset();
        }
      }
    };
  }

  private class RootWalker extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
      try {
        watchers.put(dir.toFile(), FileSystems.getDefault().newWatchService());
      } catch (IOException e) {
        log.error("failed to access filesystem directory:" + dir, e);
        return FileVisitResult.SKIP_SUBTREE;
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      makeDoc(file, Document.Operation.NEW, attrs);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      log.warn("unable to scan file " + file, exc);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      try {
        //todo investigate BarbaryWatchService for osx... even with the sun package modifier this takes 2 seconds :(
        dir.register(watchers.get(dir.toFile()), new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY}, SensitivityWatchEventModifier.HIGH);
      } catch (IOException e) {
        log.error("Failed to register watcher for:" + dir, e);
      }
      return FileVisitResult.CONTINUE;
    }
  }

  void makeDoc(Path file, Document.Operation operation, BasicFileAttributes attributes) {
    byte[] rawData = new byte[0];
    try {
      rawData = Files.readAllBytes(file);
    } catch (IOException e) {
      log.error("Could not read bytes from file:" + file, e);
    }
    String id;
    try {
      id = file.toRealPath(new LinkOption[0]).toUri().toASCIIString();
      DocumentImpl doc = new DocumentImpl(
          rawData,
          id,
          getPlan(),
          operation,
          SimpleFileWatchScanner.this
      );
      if (attributes != null) {
        doc.put("modified", String.valueOf(attributes.lastModifiedTime().toMillis()));
        doc.put("accessed", String.valueOf(attributes.lastAccessTime().toMillis()));
        doc.put("created", String.valueOf(attributes.creationTime().toMillis()));
        doc.put("file_size", String.valueOf(attributes.size()));
      }
      SimpleFileWatchScanner.this.docFound(doc);
    } catch (IOException e) {
      // TODO: perhaps we still want to proceed with non-canonical version?
      log.error("Could not resolve file path. Skipping:" + file, e);
    }
  }


  public static class Builder extends ScannerImpl.Builder {

    private SimpleFileWatchScanner obj;

    public Builder() {
      if (whoAmI() == this.getClass()) {
        obj = new SimpleFileWatchScanner();
      }
    }

    private Class whoAmI() {
      return new Object() {
      }.getClass().getEnclosingMethod().getDeclaringClass();
    }

    public Builder withRoot(File root) {
      getObject().rootDir = root;
      return this;
    }

    @Override
    public SimpleFileWatchScanner.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }

    @Override
    public SimpleFileWatchScanner.Builder outputSpace(JavaSpace outputSpace) {
      super.outputSpace(outputSpace);
      return this;
    }

    @Override
    public SimpleFileWatchScanner.Builder inputSpace(JavaSpace inputSpace) {
      super.inputSpace(inputSpace);
      return this;
    }

    @Override
    public SimpleFileWatchScanner.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public SimpleFileWatchScanner.Builder routingBy(ConfiguredBuildable<? extends Router> router) {
      super.routingBy(router);
      return this;
    }

    @Override
    public SimpleFileWatchScanner.Builder scanFreqMS(long interval) {
      super.scanFreqMS(interval);
      return this;
    }

    @Override
    public ScannerImpl build() {
      SimpleFileWatchScanner tmp = obj;
      this.obj = new SimpleFileWatchScanner();
      return tmp;
    }

    @Override
    protected SimpleFileWatchScanner getObject() {
      return obj;
    }
  }


}
