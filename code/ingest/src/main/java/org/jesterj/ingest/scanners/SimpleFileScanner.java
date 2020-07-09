/*
 * Copyright 2019 Needham Software LLC
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
import org.jesterj.ingest.config.Transient;
import org.jesterj.ingest.model.ConfiguredBuildable;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;

import java.io.File;
import java.io.IOException;
import java.lang.management.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Date;
import java.util.Optional;

/**
 * Scanner for local filesystems. This scanner periodically does a full walk of the filesystem. No persistent
 * record of files detected during walking is kept, and all files will be visited on each scan,
 * so it is highly recommended to use this with the remembering option turned on unless a regular full re-index is
 * desired. If walking the filesystem takes longer than the scan interval, the time to walk will determine
 * the index latency instead. This scanner will not start a new scan until the current one completes.
 * Files to be processed must fit in JVM memory.
 */
public class SimpleFileScanner extends ScannerImpl implements FileScanner {
  private static final Logger log = LogManager.getLogger();

  private File rootDir;
  private transient volatile boolean ready;
  private final MemoryUsage heapMemoryUsage;
  private int memWaitTimeout;
  private boolean checkDb = true;

  @SuppressWarnings("WeakerAccess")
  protected SimpleFileScanner() {
    memWaitTimeout = 30000; // default
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
  }

  @Transient
  @Override
  public Runnable getScanOperation() {
    //TODO: consider pulling this check up to ScannerImpl so its similar for
    // all ScannerImpl subclasses
    if (checkDb) {
      log.info("Checking DB");
      checkDb = false;
      return () -> {
        SimpleFileScanner.super.getScanOperation().run();
        ready = true;
      };
    } else {
      log.info("Scanning File System");
      checkDb = true;
      return () -> {
        try {
          if (isScanActive()) {
            return;
          } else  {
            log.info("Starting scan of {} at {}", this.rootDir, new Date());
          }
          // set up our watcher if needed
          scanStarted();
          this.ready = false; // ensure initial walk completes before new scans are started.
          try {
            Files.walkFileTree(rootDir.toPath(), new RootWalker());
          } catch (IOException e) {
            log.error("failed to walk filesystem!", e);
            throw new RuntimeException(e);
          } finally {
            this.ready = true;
          }
          // Process pending events. Not very likely to have concurrent scans, and even so, it doesn't
          // seem likely to cause problems.
        } catch (Exception e) {
          log.error("Exception while processing files!", e);
        } finally {
          scanFinished();
        }
      };
    }
  }

  @Override
  public Optional<Document> fetchById(String id) {
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

  @Override
  public boolean isReady() {
    return ready;
  }

  private class RootWalker extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      log.debug("found file {}", file);
      Optional<Document> document = makeDoc(file, Document.Operation.NEW, attrs);
      document.ifPresent(SimpleFileScanner.this::docFound);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFileFailed(Path file, IOException exc) {
      log.warn("unable to scan file " + file, exc);
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
      return FileVisitResult.CONTINUE;
    }
  }

  private Optional<Document> makeDoc(Path file, Document.Operation operation, BasicFileAttributes attributes) {
    byte[] rawData = new byte[0];
    try {
      long size = attributes.size();
      long memWaitStart = System.currentTimeMillis();
      int count = 0;
      while (true) {
        long l = heapMemoryUsage.getMax() - heapMemoryUsage.getUsed();
        if (!(size > l)) break;
        if ((count++ % 100) == 0){
          log.warn("waiting for memory... ({} avail {} required for next doc)",
              l, size);
        }
        // hint to the JVM that we're waiting for memory to be available
        System.gc();
        Thread.sleep(10);
        if ( System.currentTimeMillis() - memWaitStart < memWaitTimeout) {
          log.error("Unable to free up memory to load file within 30 seconds");
          log.error("Possible sources of FileScanner memory avaiability issue: " +
              "1) File is very large, " +
              "2) processing of prior files is slow or stalled, " +
              "3) Memory settings are too low");
          throw new RuntimeException("Timed out waiting for available memory to process file ("+size+" bytes):" + file);
        }
      }
      rawData = Files.readAllBytes(file);
      log.debug("Bytes Read:{}", rawData.length );
    } catch (IOException e) {
      log.error("Could not read bytes from file:" + file, e);
    } catch (InterruptedException e) {
      log.error("Document failed (not processed) due to interrupted exception", e);
      throw new RuntimeException(e);
    }
    String id;
    try {
      id = file.toRealPath().toUri().toASCIIString();
      DocumentImpl doc = new DocumentImpl(
          rawData,
          id,
          getPlan(),
          operation,
          this
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

    private SimpleFileScanner obj;

    public Builder() {
      if (whoAmI() == this.getClass()) {
        obj = new SimpleFileScanner();
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
    protected SimpleFileScanner getObj() {
      return obj;
    }

    @Override
    public SimpleFileScanner.Builder batchSize(int size) {
      super.batchSize(size);
      return this;
    }

    @Override
    public SimpleFileScanner.Builder outputSpace(JavaSpace outputSpace) {
      super.outputSpace(outputSpace);
      return this;
    }

    @Override
    public SimpleFileScanner.Builder inputSpace(JavaSpace inputSpace) {
      super.inputSpace(inputSpace);
      return this;
    }

    @Override
    public SimpleFileScanner.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public SimpleFileScanner.Builder routingBy(ConfiguredBuildable<? extends Router> router) {
      super.routingBy(router);
      return this;
    }

    @Override
    public SimpleFileScanner.Builder scanFreqMS(long interval) {
      super.scanFreqMS(interval);
      return this;
    }

    public SimpleFileScanner.Builder memoryAvailabilityTimeout(int ms) {
      getObj().memWaitTimeout = ms;
      return this;
    }

    @Override
    public ScannerImpl build() {
      SimpleFileScanner tmp = obj;
      super.build();
      this.obj = new SimpleFileScanner();
      return tmp;
    }

  }

}
