/*
 * Copyright 2014 Needham Software LLC
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

import com.google.common.io.Resources;
import net.jini.core.entry.Entry;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceMatches;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;
import net.jini.space.JavaSpace;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.docopt.clj;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.nio.charset.Charset;
import java.rmi.RMISecurityManager;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.AbstractMap;
import java.util.Map;
import java.util.Properties;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 7/5/14
 */

/**
 * Start a running instance. Each instance should have an id and a password (freely chosen
 * by the user starting the process. The ID will be used to display the node in the control
 * console and the password is meant to provide temporary security until the node is
 * configured properly.
 */
public class Main {

  private static String id;
  private static String password;

  private static final Logger log = LogManager.getLogger();

  public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
    initRMI();

    Map<String, Object> parsedArgs = usage(args);

    Properties sysprops = System.getProperties();
    for (Object prop : sysprops.keySet()) {
      log.debug(prop + "=" + sysprops.get(prop));
    }

    // This  does nothing useful yet, just for testing right now.

    password = args[0];
    System.out.println("Starting injester node...");

//
//    ServiceTemplate template = new ServiceTemplate(null, new Class[] { JavaSpace.class }, new Entry[0]);
//
//    ServiceMatches sms = sr.lookup(template, 10);
//    if(0 < sms.items.length) {
//      JavaSpace space = (JavaSpace) sms.items[0].service;
//      System.out.println(space);
//      // do something with the space
//    } else {
//      System.out.println("No Java Space found.");
//    }


//    ServiceItem si = sdm.lookup(template, null);
//    if(null != si) {
//      JavaSpace space = (JavaSpace) sms.service;
//      // do something with the space
//    } else {
//      System.out.println("No Java Space found.");
//    }


  }

  private static void initRMI() throws NoSuchFieldException, IllegalAccessException {
    // for river
    System.setProperty("java.rmi.server.RMIClassLoaderSpi", "net.jini.loader.pref.PreferredClassProvider");

    // fix bug in One-Jar with an ugly hack
    ClassLoader myClassLoader = Main.class.getClassLoader();
    String name = myClassLoader.getClass().getName();
    if ("com.simontuffs.onejar.JarClassLoader".equals(name)) {
      Field scl = ClassLoader.class.getDeclaredField("scl"); // Get system class loader
      scl.setAccessible(true); // Set accessible
      scl.set(null, myClassLoader); // Update it to your class loader
    }

    String policyFile = System.getProperty("java.security.policy");
    if (policyFile == null) {
      // for river/jni
      final Permissions pc = new Permissions();
      pc.add(new AllPermission());
      Policy.setPolicy(new Policy() {
        @Override
        public PermissionCollection getPermissions(CodeSource codesource) {
          return pc;
        }

        @Override
        public PermissionCollection getPermissions(ProtectionDomain domain) {
          return pc;
        }
      });
      System.setSecurityManager(new RMISecurityManager());
    }
  }

  private static AbstractMap<String, Object> usage(String[] args) throws IOException {
    URL usage = Resources.getResource("usage.docopts.txt");
    String usageStr = Resources.toString(usage, Charset.forName("UTF-8"));
    @SuppressWarnings("unchecked")
    AbstractMap<String, Object> result = clj.docopt(usageStr, args);
    if (result != null) {
      for (String s : result.keySet()) {
        log.debug("{}:{}", s, result.get(s));
      }
    }
    if (result == null || result.get("--help") != null) {
      System.out.println(usageStr);
      System.exit(1);
    }
    return result;
  }
}


