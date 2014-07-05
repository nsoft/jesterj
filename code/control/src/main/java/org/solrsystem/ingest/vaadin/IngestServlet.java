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

package org.solrsystem.ingest.vaadin;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/2/13
 */

import com.vaadin.server.ServiceException;
import com.vaadin.server.SessionInitEvent;
import com.vaadin.server.SessionInitListener;
import com.vaadin.server.VaadinServlet;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.ServletException;

@Singleton
public class IngestServlet  extends VaadinServlet implements SessionInitListener {


  @Inject
  private IngestUiProvider provider;

  @Override
  protected void servletInitialized() throws ServletException {
    getService().addSessionInitListener(this);
  }

  @Override
  public void sessionInit(SessionInitEvent event) throws ServiceException {
    event.getSession().addUIProvider(provider);
  }
}
