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
import org.jesterj.ingest.config.Transient;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.LinkedHashMap;
import java.util.Optional;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_DELETE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;
import static java.nio.file.StandardWatchEventKinds.OVERFLOW;

/**
 * Scanner for local filesystems. This scanner operates in a two phase process. First a full walk of the filesystem
 * is performed, and then further changes are detected by a {@link WatchService}. No persistent
 * record of files detected is kept, so while this will only send files once and then only updated/modified
 * files during it's run, restarting will send all files in the directory again.
 *
 * @deprecated JDK bugs and issues with filehandles on large repositories make this class problematic
 * see https://github.com/nsoft/jesterj/issues/130
 */
@Deprecated
public class SimpleFileWatchScanner extends ScannerImpl implements FileScanner {
  private static final Logger log = LogManager.getLogger();

  private File rootDir;
  private LinkedHashMap<File, WatchService> watchers = new LinkedHashMap<>();
  private final Object watcherLock = new Object();
  private transient volatile boolean ready;

  @SuppressWarnings("WeakerAccess")
  protected SimpleFileWatchScanner() {
  }

  @Transient
  @Override
  public Runnable getScanOperation() {
    return () -> {
      try {
        // set up our watcher if needed
        scanStarted();
        synchronized (watcherLock) {
          if (watchers.size() == 0) {
            this.ready = false; // ensure initial walk completes before new scans are started.
            try {
              Files.walkFileTree(rootDir.toPath(), new RootWalker());
            } catch (IOException e) {
              log.error("failed to walk filesystem!", e);
              throw new RuntimeException(e);
            } finally {
              this.ready = true;
            }
          }
        }
        // Process pending events. Not very likely to have concurrent scans, and even so, it doesn't
        // seem likely to cause problems.
        for (File dir : watchers.keySet()) {
          WatchService watcher = watchers.get(dir);
          for (WatchKey key; (key = watcher.poll()) != null; ) {
            for (WatchEvent<?> event : key.pollEvents()) {
              if (OVERFLOW == event.kind()) {
                //TODO need to look into what causes this and how to avoid this case...
                log.error("too many simultaneous watch events. Some filesystem were lost! ({})", key);
              }
              @SuppressWarnings("unchecked")
              WatchEvent<Path> fileEvent = (WatchEvent<Path>) event;
              Path resolvedPath = dir.toPath().resolve(fileEvent.context());
              if (resolvedPath.toFile().isDirectory()) {
                continue;
              }

              if (ENTRY_DELETE == fileEvent.kind()) {
                Optional<Document> document = makeDoc(resolvedPath, Document.Operation.DELETE, null);
                document.ifPresent(SimpleFileWatchScanner.this::docFound);
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
                Optional<Document> document = makeDoc(resolvedPath, Document.Operation.NEW, attrs);
                document.ifPresent(SimpleFileWatchScanner.this::docFound);
              }
              if (ENTRY_MODIFY == fileEvent.kind()) {
                Optional<Document> document = makeDoc(resolvedPath, Document.Operation.UPDATE, attrs);
                document.ifPresent(SimpleFileWatchScanner.this::docFound);
              }
            }
            key.reset();
          }
        }
      } catch (Exception e) {
        log.error("Exception while processing files!", e);
        // bad stuff happened, unknown state, start over.
        for (File file : watchers.keySet()) {
          try {
            watchers.get(file).close();
          } catch (IOException e2) {
            log.error("Additionally an error occured closing the watcher for {}", file);
          }
        }
        watchers.clear();
      } finally {

        scanFinished();
      }
    };
  }

  @Override
  public boolean isReady() {
    return ready;
  }

  @Override
  public Optional<Document> fetchById(String id, Object helper) {
    try {
      File file = new File(new URI(id));
      return makeDoc(file.toPath(), Document.Operation.NEW, Files.readAttributes(file.toPath(),BasicFileAttributes.class));
    } catch (URISyntaxException e) {
      log.error("Malformed doc id, can't fetch document: {}", id);
      return Optional.empty();
    } catch (IOException e) {
      log.error("Could not read file attributes! Document skipped!",e);
      return Optional.empty();
    }
  }
  private class RootWalker extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
      try {
        watchers.put(dir.toFile(), FileSystems.getDefault().newWatchService());
      } catch (IOException e) {
        log.error("failed to access filesystem directory:" + dir, e);
        return FileVisitResult.SKIP_SUBTREE;
      }
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      Optional<Document> document = makeDoc(file, Document.Operation.NEW, attrs);
      document.ifPresent(SimpleFileWatchScanner.this::docFound);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      log.warn("unable to scan file " + file, exc);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      try {
        //todo investigate BarbaryWatchService for osx... even with the sun package modifier this takes 2 seconds :(
        dir.register(watchers.get(dir.toFile()), new WatchEvent.Kind[]{ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY}, SensitivityWatchEventModifier.HIGH);
      } catch (IOException e) {
        log.error("Failed to register watcher for:" + dir, e);
      }
      return FileVisitResult.CONTINUE;
    }
  }

  private Optional<Document> makeDoc(Path file, Document.Operation operation, BasicFileAttributes attributes) {
    byte[] rawData = new byte[0];
    try {
      rawData = Files.readAllBytes(file);
    } catch (IOException e) {
      log.error("Could not read bytes from file:" + file, e);
    }
    String id;
    try {
      id = file.toRealPath().toUri().toASCIIString();
      DocumentImpl doc = new DocumentImpl(
          rawData,
          id,
          getPlan(),
          operation,
          SimpleFileWatchScanner.this
      );
      addAttrs(attributes, doc);
      return Optional.of(doc);
    } catch (IOException e) {
      // TODO: perhaps we still want to proceed with non-canonical version?
      log.error("Could not resolve file path. Skipping:" + file, e);
      return Optional.empty();
    }
  }


  public static class Builder extends ScannerImpl.Builder {

    private SimpleFileWatchScanner obj;

    public Builder() {
      if (whoAmI() == this.getClass()) {
        obj = new SimpleFileWatchScanner();
      }
    }

    @SuppressWarnings("rawtypes")
    private Class whoAmI() {
      return new Object() {
      }.getClass().getEnclosingMethod().getDeclaringClass();
    }

    public Builder withRoot(File root) {
      getObj().rootDir = root;
      return this;
    }

    @Override
    protected SimpleFileWatchScanner getObj() {
      return obj;
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
      super.build();
      this.obj = new SimpleFileWatchScanner();
      return tmp;
    }

  }


}
