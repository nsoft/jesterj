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
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 7/5/14
 */
public class Main {

  static String password;

  public static void main(String[] args) throws IOException, InterruptedException {
    URL usage = Resources.getResource("usage.docopts.txt");
    String usageStr = Resources.toString(usage, Charset.forName("UTF-8"));
    if (args.length != 1) {
      System.out.println(usageStr);
      System.exit(1);
    }
    password = args[0];
    System.out.println("Starting injester node...");

    DiscoveryManagement dlm = new LookupDiscovery(LookupDiscovery.ALL_GROUPS);

    LeaseRenewalManager lrm = new LeaseRenewalManager();
    ServiceDiscoveryManager sdm = new ServiceDiscoveryManager(dlm, lrm);

    Thread.sleep(500); //need to wait a little bit for the Lookup Service to generate the events to the sdm

    ServiceTemplate srTemplate = new ServiceTemplate(null, new Class[]{ServiceRegistrar.class}, null);

    ServiceItem[] sis = sdm.lookup(srTemplate, 10, null);
    for (ServiceItem si : sis) {
      System.out.println("Service Registrar: " + si.serviceID);
    }
    if (sis.length == 0) {
      System.out.println("No Service Registries found");
    }

    dlm.terminate();

  }
}


