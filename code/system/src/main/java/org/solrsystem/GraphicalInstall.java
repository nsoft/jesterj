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


import org.solrsystem.gui.DownloadProgress;
import org.solrsystem.gui.MessageConsole;

import javax.swing.JOptionPane;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 2/16/14
 */
public class GraphicalInstall implements InstallUserInterface, DownloadStatusListener {

  private DownloadStatusListener currentDownload;
  private MessageConsole console;

  @Override
  public boolean confirm(String message) {
    return JOptionPane.showOptionDialog(null,message,"SolrSystem Installer",
        JOptionPane.YES_NO_OPTION,JOptionPane.QUESTION_MESSAGE,null,null,null) == JOptionPane.OK_OPTION;
  }

  @Override
  public void downloadDisplayFor(String title) {
    this.currentDownload = new DownloadProgress(title);
  }

  @Override
  public void infoMessage(String message) {
    getConsole().appendLine("INFO: " + message);
  }


  @Override
  public void errorMessage(String message) {
    getConsole().appendLine("ERROR: " + message);
  }

  @Override
  public void errorMessage(String s, Throwable t) {
    errorMessage(s);
    StackTraceElement[] trace = t.getStackTrace();
    for (StackTraceElement stackTraceElement : trace) {
      getConsole().appendLine(stackTraceElement.toString());
    }
  }

  void consoleDisplayIfNeeded() {
    if (console == null) {
      console = new MessageConsole();
    }
    console.setVisible(true);
  }

  @Override
  public int progressInterval() {
    return currentDownload.progressInterval();
  }

  @Override
  public void onProgress(long bytesRead, long totalNum) {
    currentDownload.onProgress(bytesRead,totalNum);
  }

  private MessageConsole getConsole() {
    consoleDisplayIfNeeded();
    return console;
  }

}
