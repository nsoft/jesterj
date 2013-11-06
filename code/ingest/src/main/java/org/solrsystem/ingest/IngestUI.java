/*
 * Copyright 2013 Needham Software LLC
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

package org.solrsystem.ingest;

import com.vaadin.server.VaadinRequest;
import com.vaadin.ui.UI;
import org.solrsystem.ingest.vaadin.views.LogIn;

import javax.inject.Inject;

public class IngestUI extends UI {


  LogIn loginView;

  @Override
  protected void init(VaadinRequest request) {
    setContent(loginView);
  }

  @Inject
  public void setLoginView(LogIn logIn) {
    this.loginView = logIn;
  }

}