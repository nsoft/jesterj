/*
 * Copyright (c) 2014. Needham Software LLC
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 2/16/14
 */
public class TextInstall implements InstallUserInterface, DownloadStatusListener {
  @Override
  public boolean confirm(String message) {
    char response = 'a';
    while (response != 'y' && response != 'n') {
      System.out.println(message + "(y/n)");
      response = readChar();
      response = Character.toLowerCase(response);
    }
    return response == 'y';
  }

  @Override
  public void downLoad(String title) {
    System.out.println("Preparing to download " + title);
  }

  private char readChar() {
    try {
      BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
      return (char) br.read();

    } catch (IOException io) {
      io.printStackTrace();
      return (char) -1;
    }
  }

  @Override
  public int progressInterval() {
    return 1024*10;
  }

  @Override
  public void onProgress(long bytesRead, long totalnum) {
    System.out.print("\rDownloading... " +bytesRead + " of " + totalnum );
  }
}
