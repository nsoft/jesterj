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
