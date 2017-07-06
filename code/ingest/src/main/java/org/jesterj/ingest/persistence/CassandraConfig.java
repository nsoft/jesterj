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

package org.jesterj.ingest.persistence;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * A bean to represent Cassandra config. This will be converted directly to YAML so the property names are
 * constrained to use _ and lower case.
 * <p>
 * The idea is to create a default config that can generically start up on most machines and do something
 * half way sensible.
 */
public class CassandraConfig {


  private String cluster_name = "jjCassandra";
  private String listen_address = "127.0.0.1";
  private String commitlog_sync = "periodic";
  private Integer commitlog_sync_period_in_ms = 10000;
  private String partitioner = "Murmur3Partitioner";
  private String endpoint_snitch = "SimpleSnitch";
  private Boolean enable_user_defined_functions = false;
  private String commitlog_directory;
  private String hints_directory;
  private String cdc_raw_directory;
  private String saved_caches_directory;
  private String[] data_file_directories;
  private Boolean start_native_transport = Boolean.TRUE;
  private String authenticator = "PasswordAuthenticator";
  private String rpc_address = "127.0.0.1";
  private Integer num_tokens = 256;
  private Integer write_request_timeout_in_ms = 20000;

  public ArrayList getSeed_provider() {
    return seed_provider;
  }

  public void setSeed_provider(ArrayList<Map<String, Object>> seed_provider) {
    this.seed_provider = seed_provider;
  }

  private ArrayList<Map<String, Object>> seed_provider;

  @SuppressWarnings("unchecked")
  public CassandraConfig(String cassandraDir) {

    commitlog_directory = cassandraDir + "/data/commitlog";
    hints_directory = cassandraDir + "/data/hints";
    cdc_raw_directory = cassandraDir + "/data/hints";
    saved_caches_directory = cassandraDir + "/data/saved_caches";
    data_file_directories = new String[]{cassandraDir + "/data/data"};

    // yuck, but it's what's required.
    seed_provider = new ArrayList();
    HashMap<String, Object> m = new HashMap<>();
    seed_provider.add(m);
    m.put("class_name", "org.apache.cassandra.locator.SimpleSeedProvider");
    ArrayList<Map<String, String>> l = new ArrayList<>();
    m.put("parameters", l);
    HashMap<String, String> m2 = new HashMap<>();
    l.add(m2);
    m2.put("seeds", getListen_address());

  }

  public String guessIp() {
    System.out.println("Choosing IP");
    for (String host : listAdresses()) {
      System.out.println(host);
      if (better(host)) {
        System.out.println(host + " is better");
        setListen_address(host);
      }
    }
    String listen_address = getListen_address();
    System.out.println("Cassandra will listen on " + listen_address);
    return listen_address;
  }

  // No unit test for this because of final class NetworkInterface.
  Set<String> listAdresses() {
    HashSet<String> adresses = new HashSet<>();
    try {
      Enumeration e = networkInterfaces();
      while (e.hasMoreElements()) {
        NetworkInterface n = (NetworkInterface) e.nextElement();
        Enumeration ee = n.getInetAddresses();
        while (ee.hasMoreElements()) {
          InetAddress i = (InetAddress) ee.nextElement();
          adresses.add(i.getHostAddress());
        }
      }
    } catch (SocketException e1) {
      e1.printStackTrace();
    }
    return adresses;
  }

  Enumeration<NetworkInterface> networkInterfaces() throws SocketException {
    return NetworkInterface.getNetworkInterfaces();
  }

  /**
   * Bigger is better. Wider address spaces that are not part of multicast are preferred, except technically
   * private IPV6 is bigger than public, but public addresses are preferred.
   *
   * @param newHost the numeric host adress
   * @return true if the address space is better than the present value in this bean.
   */
  boolean better(String newHost) {

    if (Network.CLASS_D_MULTICAST.is(newHost) || Network.IPV6_LINKLOCAL.is(newHost)) {
      // ignore all multicast addresses.
      return false;
    }

    String host = this.getListen_address();
    if (Network.PUBLIC.is(newHost) && !Network.PUBLIC.is(host)) {
      return true;
    }
    if (Network.IPV6_PRIVATE.is(newHost) &&
        (Network.CLASS_A_PRIVATE.is(host)
            || Network.CLASS_B_PRIVATE.is(host)
            || Network.CLASS_C_PRIVATE.is(host)
            || Network.LOCALHOST.is(host))) {
      return true;
    }
    if (Network.CLASS_A_PRIVATE.is(newHost) &&
        (Network.CLASS_B_PRIVATE.is(host)
            || Network.CLASS_C_PRIVATE.is(host)
            || Network.LOCALHOST.is(host))) {
      return true;
    }
    if (Network.CLASS_B_PRIVATE.is(newHost) &&
        (Network.CLASS_C_PRIVATE.is(host)
            || Network.LOCALHOST.is(host))) {
      return true;
    }
    // don't "simplify" as it becomes unreadable.
    //noinspection RedundantIfStatement
    if (Network.CLASS_C_PRIVATE.is(newHost) && Network.LOCALHOST.is(host)) {
      return true;
    }

    return false;
  }

  public String getCluster_name() {
    return cluster_name;
  }

  public void setCluster_name(String cluster_name) {
    this.cluster_name = cluster_name;
  }

  public String getListen_address() {
    return listen_address;
  }

  @SuppressWarnings("unchecked")
  public void setListen_address(String listen_address) {
    this.listen_address = listen_address;
    this.rpc_address = listen_address;
    // This is awfull... :(
    ((List<Map<String, String>>) this.seed_provider.get(0).get("parameters")).get(0).put("seeds", listen_address);

  }

  public String getCommitlog_sync() {
    return commitlog_sync;
  }

  public void setCommitlog_sync(String commitlog_sync) {
    this.commitlog_sync = commitlog_sync;
  }

  public Integer getCommitlog_sync_period_in_ms() {
    return commitlog_sync_period_in_ms;
  }

  public void setCommitlog_sync_period_in_ms(Integer commitlog_sync_period_in_ms) {
    this.commitlog_sync_period_in_ms = commitlog_sync_period_in_ms;
  }

  public String getPartitioner() {
    return partitioner;
  }

  public void setPartitioner(String partitioner) {
    this.partitioner = partitioner;
  }

  public String getEndpoint_snitch() {
    return endpoint_snitch;
  }

  public void setEndpoint_snitch(String endpoint_snitch) {
    this.endpoint_snitch = endpoint_snitch;
  }

  public String getCommitlog_directory() {
    return commitlog_directory;
  }

  public void setCommitlog_directory(String commitlog_directory) {
    this.commitlog_directory = commitlog_directory;
  }

  public String getHints_directory() {
    return hints_directory;
  }

  public void setHints_directory(String hints_directory) {
    this.hints_directory = hints_directory;
  }
  public String getSaved_caches_directory() {
    return saved_caches_directory;
  }

  public void setSaved_caches_directory(String saved_caches_directory) {
    this.saved_caches_directory = saved_caches_directory;
  }

  public String[] getData_file_directories() {
    return data_file_directories;
  }

  public void setData_file_directories(String[] data_file_directories) {
    this.data_file_directories = data_file_directories;
  }

  public Boolean getStart_native_transport() {
    return start_native_transport;
  }

  public void setStart_native_transport(Boolean start_native_transport) {
    this.start_native_transport = start_native_transport;
  }

  public String getAuthenticator() {
    return authenticator;
  }

  public void setAuthenticator(String authenticator) {
    this.authenticator = authenticator;
  }

  public String getRpc_address() {
    return rpc_address;
  }

  public void setRpc_address(String rpc_address) {
    this.rpc_address = rpc_address;
  }

  public Integer getNum_tokens() {
    return num_tokens;
  }

  public void setNum_tokens(Integer num_tokens) {
    this.num_tokens = num_tokens;
  }

  public Integer getWrite_request_timeout_in_ms() {
    return write_request_timeout_in_ms;
  }

  public void setWrite_request_timeout_in_ms(Integer write_request_timeout_in_ms) {
    this.write_request_timeout_in_ms = write_request_timeout_in_ms;
  }

  public String getCdc_raw_directory() {
    return cdc_raw_directory;
  }

  public void setCdc_raw_directory(String cdc_raw_directory) {
    this.cdc_raw_directory = cdc_raw_directory;
  }

  public Boolean getEnable_user_defined_functions() {
    return enable_user_defined_functions;
  }

  public void setEnable_user_defined_functions(Boolean enable_user_defined_functions) {
    this.enable_user_defined_functions = enable_user_defined_functions;
  }


  private enum Network {
    PUBLIC {
      @Override
      public boolean is(String addr) {
        boolean a = CLASS_A_PRIVATE.is(addr);
        boolean b = CLASS_B_PRIVATE.is(addr);
        boolean c = CLASS_C_PRIVATE.is(addr);
        boolean local = LOCALHOST.is(addr);
        boolean ipv6p = IPV6_PRIVATE.is(addr);
        return !ipv6p && !a && !b && !c && !local;
      }
    }, IPV6_PRIVATE {
      @Override
      public boolean is(String addr) {
        return (addr.toLowerCase().startsWith("fd"));
      }
    },

    CLASS_A_PRIVATE {
      @Override
      public boolean is(String addr) {
        return addr.startsWith("10.");
      }
    },
    CLASS_B_PRIVATE {
      @Override
      public boolean is(String addr) {
        if (addr.contains(".")) {
          String[] parts = addr.split("\\.");
          int part2 = Integer.parseInt(parts[1]);
          return part2 > 15 && part2 < 32;
        } else {
          return false;
        }
      }
    },
    CLASS_C_PRIVATE {
      @Override
      public boolean is(String addr) {
        return addr.startsWith("192.168");
      }
    },
    LOCALHOST {
      @Override
      public boolean is(String addr) {
        return
            "127.0.0.1".equals(addr)
                || "::1".equals(addr)
                || "0:0:0:0:0:0:0:1".equals(addr)
                || "0000:0000:0000:0000:0000:0000:0000:0001".equals(addr);
      }
    },
    CLASS_D_MULTICAST {
      @Override
      public boolean is(String addr) {
        if (addr.contains(".")) {
          String[] parts = addr.split("\\.");
          int part1 = Integer.parseInt(parts[0]);
          return part1 > 223 && part1 < 239;
        } else {
          return false;
        }
      }
    },

    IPV6_LINKLOCAL {
      @Override
      public boolean is(String addr) {
        return (addr.toLowerCase().startsWith("fe80"));
      }
    };

    public abstract boolean is(String addr);
  }

}
