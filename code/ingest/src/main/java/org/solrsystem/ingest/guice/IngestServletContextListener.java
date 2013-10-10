package org.solrsystem.ingest.guice;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/5/13
 * Time: 9:18 AM
 */

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.solrsystem.ingest.shiro.IngestShiroModule;
import org.solrsystem.ingest.vaadin.VaadinModule;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

public class IngestServletContextListener extends GuiceServletContextListener {
  private ServletContext servletContext;

  @Override
  public void contextInitialized(ServletContextEvent servletContextEvent) {
    servletContext = servletContextEvent.getServletContext();
    super.contextInitialized(servletContextEvent);
  }

  @Override
  protected Injector getInjector() {
    return Guice.createInjector(new IngestShiroModule(servletContext), new VaadinModule());
  }
}
