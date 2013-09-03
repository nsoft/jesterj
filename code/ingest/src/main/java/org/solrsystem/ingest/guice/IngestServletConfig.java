package org.solrsystem.ingest.guice;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/2/13
 * Time: 8:38 PM
 */

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.servlet.GuiceServletContextListener;
import org.solrsystem.ingest.vaadin.VaadinModule;

import javax.servlet.annotation.WebListener;

@WebListener
public class IngestServletConfig extends GuiceServletContextListener{
  private static Injector INJECTOR = Guice.createInjector(new VaadinModule());

  @Override
  protected  Injector getInjector() {
    return INJECTOR;
  }


  static Injector injector() {
    return INJECTOR;
  }
}
