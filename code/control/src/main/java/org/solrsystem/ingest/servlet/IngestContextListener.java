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
