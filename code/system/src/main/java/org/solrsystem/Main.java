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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

public class Main {

  public static final String GRADLE = "http://services.gradle.org/distributions/gradle-1.11-bin.zip";

  private static InstallUserInterface UI;

  public static void main(String[] args) throws IOException, HttpException {

    if ( GraphicsEnvironment.isHeadless()) {
      UI = new TextInstall();
    } else {
      UI = new GraphicalInstall();
    }

    if (UI.confirm("Do you want to create ./SolrSystem in this directory and install SolrSystem?")) {
      File solrSystemDir = new File("SolrSystem");
      //noinspection ResultOfMethodCallIgnored
      solrSystemDir.mkdir();
      UI.downLoad(GRADLE);

      File file = new File(solrSystemDir, "gradle-1.11-bin.zip");
      if (!file.exists()) {
        doGetToFile(GRADLE, file.getCanonicalPath(), UI);
      }
    }


  }

  public static boolean doGetToFile(String url, String localFilePath, DownloadStatusListener listener) throws HttpException, IOException {
    final HttpGet request = new HttpGet(url);
    final HttpResponse resp;
    try {
      DefaultHttpClient httpClient = new DefaultHttpClient();



      resp = httpClient.execute(request);
      long totalnum=resp.getEntity().getContentLength();
      if (resp.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
        FileOutputStream out = new FileOutputStream(localFilePath);

        InputStream inputStream=resp.getEntity().getContent();

        long bytesRead=0;
        int bufferSize=listener.progressInterval();
        byte b[]=new byte[bufferSize];

        int cnt;
        System.out.println("reading " + bufferSize);
        while((cnt=inputStream.read(b))!=-1)
        {
          out.write(b,0,cnt);
          bytesRead+=cnt;
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

}