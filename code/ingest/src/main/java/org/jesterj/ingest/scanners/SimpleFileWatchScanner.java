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

import net.jini.space.JavaSpace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.model.impl.StepImpl;

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
import java.nio.file.attribute.BasicFileAttributes;
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
  WatchService watcher;
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
        if (watcher == null) {
          try {
            watcher = FileSystems.getDefault().newWatchService();
          } catch (IOException e) {
            log.error("failed to access filesystem!", e);
            throw new IllegalStateException("Cannot access directory: " + rootDir, e);
          }
          try {
            Files.walkFileTree(rootDir.toPath(), new RootWalker());
          } catch (IOException e) {
            watcher = null;
            log.error("failed to walk filesystem!", e);
            throw new RuntimeException(e);
          }
        }
      }
      // Process pending events
      for (WatchKey key; (key = watcher.poll()) != null; ) {
        for (WatchEvent<?> event : key.pollEvents()) {
          if (OVERFLOW == event.kind()) {
            // need to look into avoiding this case...
            log.error("too many simultaneous watch events. Some filesystem were lost!", key);
          }
          @SuppressWarnings("unchecked")
          WatchEvent<Path> fileEvent = (WatchEvent<Path>) event;
          if (ENTRY_CREATE == fileEvent.kind()) {
            makeDoc(fileEvent.context(), Document.Operation.NEW);
          }
          if (ENTRY_MODIFY == fileEvent.kind()) {
            makeDoc(fileEvent.context(), Document.Operation.UPDATE);
          }
          if (ENTRY_DELETE == fileEvent.kind()) {
            makeDoc(fileEvent.context(), Document.Operation.DELETE);
          }
        }
      }
    };
  }

  private class RootWalker extends SimpleFileVisitor<Path> {

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
      makeDoc(file, Document.Operation.NEW);
      return super.visitFile(file, attrs);
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
      log.warn("unable to scan file", exc);
      return super.visitFileFailed(file, exc);
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
      try {
        dir.register(watcher, ENTRY_CREATE, ENTRY_DELETE, ENTRY_MODIFY);
      } catch (IOException e) {
        log.error("Failed to register watcher for:" + dir, e);
      }
      return super.postVisitDirectory(dir, exc);
    }
  }

  void makeDoc(Path file, Document.Operation operation) {
    byte[] rawData = new byte[0];
    try {
      rawData = Files.readAllBytes(file);
    } catch (IOException e) {
      log.error("Could not read bytes from file:" + file, e);
    }
    String id;
    try {
      id = file.toRealPath(new LinkOption[0]).toUri().toASCIIString();
      SimpleFileWatchScanner.this.docFound(
          new DocumentImpl(
              rawData,
              id,
              getPlan(),
              operation,
              SimpleFileWatchScanner.this
          )
      );
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
    public StepImpl.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }

    @Override
    public StepImpl.Builder outputSpace(JavaSpace outputSpace) {
      super.outputSpace(outputSpace);
      return this;
    }

    @Override
    public StepImpl.Builder inputSpace(JavaSpace inputSpace) {
      super.inputSpace(inputSpace);
      return this;
    }

    @Override
    public StepImpl.Builder stepName(String stepName) {
      super.stepName(stepName);
      return this;
    }

    @Override
    public StepImpl.Builder routingBy(Router router) {
      super.routingBy(router);
      return this;
    }

    @Override
    public ScannerImpl.Builder interval(long interval) {
      super.interval(interval);
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
