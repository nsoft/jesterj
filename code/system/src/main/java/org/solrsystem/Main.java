/*
 * Copyright (c) 2013. Needham Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.solrsystem;


import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import java.awt.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

class Main {

  private static final String GRADLE = "http://services.gradle.org/distributions/gradle-1.11-bin.zip";

  private static InstallUserInterface UI;

  public static void main(String[] args) throws IOException, HttpException {
    new Main().install();
  }

  public void install() {
    selectUI();

    if (userSaysOkToInstall()) {
      File solrSystemDir = createSolrSystemHome();
      downloadGradle(solrSystemDir);
      File toolsDir = createToolsDir(solrSystemDir);
      unpackGradleInto(toolsDir);
    }
  }

  void unpackGradleInto(File toolsDir) {

  }

  File createToolsDir(File solrSystemDir) {
    File toolsDir = new File(solrSystemDir, "tools");
    String canonicalPath;
    try {
      canonicalPath = toolsDir.getCanonicalPath();
    } catch (IOException e) {
      canonicalPath = toolsDir.getAbsolutePath() + " (non-canonical)";
    }
    if (toolsDir.mkdir()) {
      UI.infoMessage("Created tools dir: " + canonicalPath);
    } else {
      UI.errorMessage("Could not create tools dir: " + canonicalPath);
    }
    return toolsDir;
  }

  void downloadGradle(File solrSystemDir) {
    UI.downloadDisplayFor(GRADLE);

    File file = new File(solrSystemDir, "gradle-1.11-bin.zip");
    if (!file.exists()) {
      try {
        doGetToFile(GRADLE, file.getCanonicalPath(), UI);
      } catch (HttpException e) {
        e.printStackTrace();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  File createSolrSystemHome() {
    File solrSystemDir = new File("SolrSystem");
    //noinspection ResultOfMethodCallIgnored
    solrSystemDir.mkdir();
    return solrSystemDir;
  }

  boolean userSaysOkToInstall() {
    return UI.confirm("Do you want to create ./SolrSystem in this directory and install SolrSystem?");
  }

  void selectUI() {
    if (GraphicsEnvironment.isHeadless()) {
      UI = new TextInstall();
    } else {
      UI = new GraphicalInstall();
    }
  }

  boolean doGetToFile(String url, String localFilePath, DownloadStatusListener listener) throws HttpException {
    final HttpGet request = new HttpGet(url);
    final HttpResponse resp;
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();


      resp = httpClient.execute(request);
      long totalnum = resp.getEntity().getContentLength();
      if (resp.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        FileOutputStream out = new FileOutputStream(localFilePath);

        InputStream inputStream = resp.getEntity().getContent();

        long bytesRead = 0;
        int bufferSize = listener.progressInterval();
        byte b[] = new byte[bufferSize];

        int cnt;
        System.out.println("reading " + bufferSize);
        while ((cnt = inputStream.read(b)) != -1) {
          out.write(b, 0, cnt);
          bytesRead += cnt;
          listener.onProgress(bytesRead, totalnum);
        }
        out.flush();
        out.close();

        //resp.getEntity().writeTo(out);

        out.close();
        return true;
      } else {
        System.out.println("Download Failed:" + resp.getStatusLine());
        return false;
      }
    } catch (final IOException e) {
      e.printStackTrace();
      throw new HttpException("IOException " + e.toString());
    }
  }

  public void unzipFile(File zipFile, File destDir) throws IOException {
    ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
    ZipEntry entry = zis.getNextEntry();
    while (entry != null) {
      File entryFile = new File(destDir, entry.getName());
      Files.copy(zis, entryFile.toPath(),
          StandardCopyOption.REPLACE_EXISTING,
          StandardCopyOption.COPY_ATTRIBUTES);
      entry = zis.getNextEntry();
    }
  }

}