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

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 2/16/14
 */

interface InstallUserInterface extends DownloadStatusListener{
  public boolean confirm(String message);
  public void downloadDisplayFor(String title);
  public void infoMessage(String message);
  public void errorMessage(String s);
  public void errorMessage(String s, Throwable t);
}
