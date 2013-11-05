package org.solrsystem.ingest.guice;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/5/13
 * Time: 9:18 AM
 */

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;

import javax.servlet.ServletContextEvent;

public class IngestServletContextListener extends GuiceServletContextListener {

  @Override
  public void contextInitialized(ServletContextEvent servletContextEvent) {
    super.contextInitialized(servletContextEvent);
  }

  private static Injector INJECTOR = Guice.createInjector(new VaadinModule());

  @Override
  protected  Injector getInjector() {
    return INJECTOR;
  }


  public static Injector injector() {
    return INJECTOR;
  }

}
