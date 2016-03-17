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
import org.jesterj.ingest.model.Step;
import org.jesterj.ingest.model.impl.ScannerImpl;
import org.jesterj.ingest.model.impl.StepImpl;

import java.io.File;
import java.nio.file.WatchService;
import java.util.Set;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 1/27/16
 */

/**
 * Scanner for local filesystems. Procedes in a two phase process. First a full walk of the filesystem (ignoring
 * symbolic links by default) is performed, and then further changes are detected by a set of
 * {@link java.nio.file.WatchService}
 */
public class FileWatchScanner extends ScannerImpl {

  private File rootDir;
  private Set<WatchService> watchedDirs;

  protected FileWatchScanner() {
  }
  
  

  public static class Builder extends ScannerImpl.Builder {

    private FileWatchScanner obj = new FileWatchScanner();

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
    public StepImpl.Builder nextStep(Step next) {
      super.nextStep(next);
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
    public ScannerImpl.Builder interval(long interval) {
      super.interval(interval);
      return this;
    }

    @Override
    public ScannerImpl build() {
      FileWatchScanner tmp = obj;
      this.obj = new FileWatchScanner();
      return tmp;
    }

    @Override
    protected FileWatchScanner getObject() {
      return obj;
    }
  }


}
