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

package org.jesterj.ingest;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/11/14
 */

import net.jini.core.lookup.ServiceID;
import net.jini.core.lookup.ServiceItem;
import net.jini.core.lookup.ServiceRegistrar;
import net.jini.core.lookup.ServiceTemplate;
import net.jini.discovery.DiscoveryManagement;
import net.jini.discovery.LookupDiscovery;
import net.jini.lease.LeaseRenewalManager;
import net.jini.lookup.ServiceDiscoveryManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.jini.service.IngestService;
import org.jesterj.ingest.model.Plan;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Don't take this serious... I was just messing around.
 */
public class IngestNode implements Runnable, IngestService {

  private static final Logger log = LogManager.getLogger();

  private final String id;
  private final String password;
  private Map<ServiceID, ServiceRegistrar> ingestNodes = new LinkedHashMap<>();

  public IngestNode(String id, String password) {

    this.id = id;
    this.password = password;
  }


  @Override
  public void run() {
    DiscoveryManagement dlm = null;
    ServiceDiscoveryManager sdm = null;
    try {
      dlm = new LookupDiscovery(LookupDiscovery.ALL_GROUPS);
      LeaseRenewalManager lrm = new LeaseRenewalManager();
      sdm = new ServiceDiscoveryManager(dlm, lrm);
    } catch (IOException e) {
      e.printStackTrace();
      System.exit(2);
    }

    ServiceTemplate srTemplate = new ServiceTemplate(null, new Class[]{ServiceRegistrar.class}, null);

    log.info("Now Scanning for Service Registries...");
    while (true) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        dlm.terminate();
        break;
      }
      ServiceItem[] sis = sdm.lookup(srTemplate, 10, null);
      Map<ServiceID, ServiceRegistrar> registrars = new LinkedHashMap<>();
      for (ServiceItem si : sis) {
        ServiceRegistrar registrar = (ServiceRegistrar) si.service;
        ServiceRegistrar old = registrars.put(registrar.getServiceID(), registrar);
        if (old == null) {
          log.debug("added: " + registrar.getServiceID());
        } else {
          log.debug("replaced: " + registrar.getServiceID());
        }
      }
      if (sis.length == 0) {
        log.trace("No Service Registries found");
      }
    }
  }

  @Override
  public void supplySpace() {
    // Todo
  }

  @Override
  public void installPlan(Plan plan) {

  }

  @Override
  public void activate(Plan plan) {

  }

  @Override
  public void deactivate(Plan plan) {

  }
}
