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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.Router;
import org.jesterj.ingest.model.impl.DocumentImpl;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.routers.RouterBase;
import org.jetbrains.annotations.NotNull;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scanner for local filesystems. This scanner periodically does a full walk of the filesystem. No persistent
 * record of files detected during walking is kept, and all files will be visited on each scan,
 * so it is highly recommended to use this with the remembering option turned on unless a regular full re-index is
 * desired. If walking the filesystem takes longer than the scan interval, the time to walk will determine
 * the index latency instead. This scanner will not start a new scan until the current one completes.
 * Files to be processed must fit in JVM memory.
 */
@SuppressWarnings("SameParameterValue")
public class SimpleFileScanner extends ScannerImpl implements FileScanner {
  private static final Logger log = LogManager.getLogger();
  private final AtomicInteger opCountTrace = new AtomicInteger(0);

  private static final Object SCAN_LOCK = new Object();
  private File rootDir;
  private transient volatile boolean scanning;
  private FileFilter includes;
  private FileFilter docPerLine;
  private final MemoryUsage heapMemoryUsage;
  private int memWaitTimeout;

  @SuppressWarnings("WeakerAccess")
  protected SimpleFileScanner() {
    memWaitTimeout = 30000; // default
    MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
    heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
  }

  @Override
  public ScanOp getScanOperation() {
    return new ScanOp(() -> {
      log.trace("Scan Op:{}" , opCountTrace::incrementAndGet);
      synchronized (SimpleFileScanner.SCAN_LOCK) {
        log.trace("Acquired lock on " + SimpleFileScanner.this);
        setScanning(true); // ensure initial walk completes before new scans are started.
        try {
          log.trace("About to walk");
          Files.walkFileTree(rootDir.toPath(), new RootWalker());
          log.trace("FileWalk complete");
        } catch (IOException e) {
          log.error("failed to walk filesystem!", e);
          throw new RuntimeException(e);
        } finally {
          processDirty();
          setScanning(false);
        }
      }
    }, this);
  }

  @Override
  public boolean isScanning() {
    return this.scanning;
  }

  @Override
  public Optional<Document> fetchById(String id, String origination) {
    try {
      String fileForId = id;
      if (id.contains("#")) {
        fileForId = fileForId.substring(0,id.indexOf("#"));
      }
      File file;
      try {
         file = new File(new URI(fileForId));
      } catch (IllegalArgumentException e) {
        // stupid jdk errors that don't tell you what the input was
        log.info("JDK error message stupid:",e);
        log.info("Input was {}",fileForId);
        throw e;
      }
      // todo: this will be inefficient, but for the moment bank on restarts only having a small number of docs in flight
      BasicFileAttributes attributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
      if (docPerLine != null && docPerLine.accept(file)) {
        if (!id.contains("#")) {
          log.error("Could not interpret line by line file lacking fragment identifier to denote line");
          return Optional.empty();
        }
        String lineStr = id.split("#")[1];
        if (!lineStr.startsWith("L")) {
          log.error("Invalid line number{} in ID:{}", lineStr, id);
        }
        long lineNo = Long.parseLong(lineStr.substring(1));
        try(LineNumberReader reader = new LineNumberReader(new FileReader(file))) {
          while (true) {
            String line = reader.readLine();
            if (line == null) {
              break;
            }
            if (reader.getLineNumber() < lineNo) {
              continue;
            }
            return Optional.of(docWithAttrs(Document.Operation.NEW, attributes, FTI_ORIGIN, line.getBytes(), id));
          }
          log.error("Not found: {}" , id);
          return Optional.empty();
        }
      } else {
        return makeDoc(file.toPath(), Document.Operation.NEW, attributes, origination);
      }
    } catch (URISyntaxException e) {
      log.error("Malformed doc id, can't fetch document: {}", id);
      return Optional.empty();
    } catch (IOException e) {
      log.error("Could not read file attributes! Document skipped!", e);
      return Optional.empty();
    }
  }


  protected void setScanning(boolean scanning) {
    synchronized (SCAN_LOCK) {
      this.scanning = scanning;
    }
  }

  private class RootWalker extends SimpleFileVisitor<Path> {
    @Override
    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
      return FileVisitResult.CONTINUE;
    }

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
      log.trace("found file {}", file);
      File asFile = file.toFile();
      if (includes == null || includes.accept(asFile)) {
        if (docPerLine != null && docPerLine.accept(asFile)) {
          makeLineDocs(file,Document.Operation.NEW,attrs,SCAN_ORIGIN);
        } else {
          Optional<Document> document = makeDoc(file, Document.Operation.NEW, attrs, SCAN_ORIGIN);
          log.trace("Created:{}", document::get);
          document.ifPresent(SimpleFileScanner.this::docFound);
        }
      }
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

  private void makeLineDocs(Path file, Document.Operation operation, BasicFileAttributes attributes, String origination) {
    try(LineNumberReader reader = new LineNumberReader(new FileReader(file.toFile()))) {
      long bytesRead = 0;
      while (true) {
        byte[] rawData;
        String line = reader.readLine();
        if (line == null) {
            break;
        }
        rawData = line.getBytes();
        bytesRead += (long) line.length() *Character.BYTES;
        long size = bytesRead / reader.getLineNumber();
        memThrottle(size, "Timed out waiting for available memory to process file (" + size + " bytes):" + file);
        String id = file.toRealPath().toUri().toASCIIString()+ "#L" + reader.getLineNumber();
        DocumentImpl doc = docWithAttrs(operation, attributes, origination, rawData, id);
        doc.put("__LINE_NUMBER__", String.valueOf(reader.getLineNumber()));
        log.trace("Bytes Read:{}", rawData.length);
        docFound(doc);
      }
    } catch (IOException e) {
      log.error("Could not read bytes from file:" + file, e);
    } catch (InterruptedException e) {
      log.error("Document failed (not fully processed) due to interrupted exception", e);
      throw new RuntimeException(e);
    }
  }

  @NotNull
  private DocumentImpl docWithAttrs(Document.Operation operation, BasicFileAttributes attributes, String origination, byte[] rawData, String id) {
    DocumentImpl doc = new DocumentImpl(
        rawData,
        id,
        getPlan(),
        operation,
        this,
        origination
    );
    addAttrs(attributes, doc);
    return doc;
  }

  private Optional<Document> makeDoc(Path file, Document.Operation operation, BasicFileAttributes attributes, String origination) {
    byte[] rawData = new byte[0];
    try {
      long size = attributes.size();
      memThrottle(size, "Timed out waiting for available memory to process file (" + size + " bytes):" + file);
      rawData = Files.readAllBytes(file);
      log.trace("Bytes Read:{}", rawData.length);
    } catch (IOException e) {
      log.error("Could not read bytes from file:" + file, e);
    } catch (InterruptedException e) {
      log.error("Document failed (not processed) due to interrupted exception", e);
      throw new RuntimeException(e);
    }
    String id;
    try {
      id = file.toRealPath().toUri().toASCIIString();
      DocumentImpl doc = docWithAttrs(operation, attributes, origination, rawData, id);
      return Optional.of(doc);
    } catch (IOException e) {
      log.error("Could not resolve file path. Skipping:" + file, e);
      return Optional.empty();
    }
  }

  private void memThrottle(long size, String message) throws InterruptedException {
    long memWaitStart = System.currentTimeMillis();
    int count = 0;
    while (true) {
      long l = heapMemoryUsage.getMax() - heapMemoryUsage.getUsed();
      if (!(size > l)) break;
      if ((count++ % 100) == 0) {
        log.warn("waiting for memory... ({} avail {} required for next doc)",
            l, size);
      }
      // hint to the JVM that we're waiting for memory to be available
      System.gc();
      //noinspection BusyWait
      Thread.sleep(10);
      if (System.currentTimeMillis() - memWaitStart < memWaitTimeout) {
        log.error("Unable to free up memory to load file within {} seconds", memWaitStart / 1000);
        log.error("Possible sources of FileScanner memory availability issue: " +
            "1) File is very large, " +
            "2) processing of prior files is slow or stalled, " +
            "3) Memory settings are too low");
        //noinspection BusyWait
        Thread.sleep(100);
        RuntimeException runtimeException = new RuntimeException(message);
        runtimeException.printStackTrace();
        throw runtimeException;
      }
    }
  }

  @SuppressWarnings("unused")
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
    public SimpleFileScanner.Builder named(String stepName) {
      super.named(stepName);
      return this;
    }

    @Override
    public SimpleFileScanner.Builder routingBy(RouterBase.Builder<? extends Router> router) {
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

    public SimpleFileScanner.Builder acceptOnly(FileFilter filter) {
      getObj().includes = filter;
      return this;
    }

    public SimpleFileScanner.Builder docPerLineIfMatches(FileFilter filter) {
      getObj().docPerLine = filter;
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
