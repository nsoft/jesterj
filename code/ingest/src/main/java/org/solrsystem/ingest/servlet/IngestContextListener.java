package org.solrsystem.ingest.servlet;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/10/13
 * Time: 10:32 AM
 */

import org.apache.shiro.SecurityUtils;
import org.apache.shiro.config.IniSecurityManagerFactory;
import org.apache.shiro.mgt.SecurityManager;
import org.apache.shiro.util.Factory;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

@WebListener
public class IngestContextListener implements ServletContextListener {
  @Override
  public void contextInitialized(ServletContextEvent sce) {
    // Use the shiro.ini file at the root of the classpath
    // (file: and url: prefixes load from files and urls respectively):
    Factory<SecurityManager> factory = new IniSecurityManagerFactory("classpath:shiro.ini");
    SecurityManager securityManager = factory.getInstance();

    // Since Vaadin doesn't really base its UI on distinct URL paths we will eschew
    // shiro web module entirely, we just don't need it.
    SecurityUtils.setSecurityManager(securityManager);

  }

  @Override
  public void contextDestroyed(ServletContextEvent sce) {
    //nothing
  }
}
