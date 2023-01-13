package org.jesterj.ingest.persistence;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.NativeAccessMBean;
import org.apache.cassandra.utils.MBeanWrapper;
import org.apache.cassandra.utils.NativeLibrary;

import javax.management.StandardMBean;
import java.io.File;
import java.util.UUID;

import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_FOREGROUND;
import static org.apache.cassandra.config.CassandraRelevantProperties.CASSANDRA_PID_FILE;

/**
 * A customized version of the cassandra class that does not use a logger and does a better job of
 * shutting down so that we can start it more than once in our tests.
 */
class JJCassandraDaemon extends CassandraDaemon {
  public static final String MBEAN_NAME = "org.jesterj.cassandra.db:type=NativeAccess";
  private final boolean runManaged = true;

  public JJCassandraDaemon() {
    super(true);
  }

  private void exitOrFail(String message, Throwable cause) {
    if (runManaged) {
      @SuppressWarnings("UnnecessaryLocalVariable") // for debugging
      RuntimeException t = cause != null ? new RuntimeException(message, cause) : new RuntimeException(message);
      throw t;
    } else {
      System.err.println(message);
      cause.printStackTrace();
      System.exit(3);
    }
  }

  @Override
  public void stop() {
    super.stop();
    ActiveRepairService.instance.stop();
  }

  public void activate() {
    // Do not put any references to DatabaseDescriptor above the forceStaticInitialization call.
    try {
      applyConfig();

      registerNativeAccess();

      setup();

      String pidFile = CASSANDRA_PID_FILE.getString();

      if (pidFile != null) {
        new File(pidFile).deleteOnExit();
      }

      if (CASSANDRA_FOREGROUND.getString() == null) {
        System.out.close();
        System.err.close();
      }

      start();

      System.out.println("Startup complete");
    } catch (Throwable e) {
      boolean logStackTrace = !(e instanceof ConfigurationException) || ((ConfigurationException) e).logStackTrace;

      System.out.println("Exception (" + e.getClass().getName() + ") encountered during startup: " + e.getMessage());

      if (logStackTrace) {
        if (runManaged)
          System.err.println("Exception encountered during startup" + e);

        // try to warn user on stdout too, if we haven't already detached
        e.printStackTrace();
        exitOrFail("Exception encountered during startup", e);
      } else {
        if (runManaged)
          System.err.println("Exception encountered during startup: {}" + e.getMessage());
        // try to warn user on stdout too, if we haven't already detached
        e.printStackTrace();
        System.err.println(e.getMessage());
        exitOrFail("Exception encountered during startup: " + e.getMessage(), null);
      }
    }
  }
  public static void registerNativeAccess() throws javax.management.NotCompliantMBeanException
  {
    MBeanWrapper.instance.registerMBean(new StandardMBean(new NativeAccess(), NativeAccessMBean.class), MBEAN_NAME + UUID.randomUUID(), MBeanWrapper.OnException.LOG);
  }

  static class NativeAccess implements NativeAccessMBean
  {
    public boolean isAvailable()
    {
      return NativeLibrary.isAvailable();
    }

    public boolean isMemoryLockable()
    {
      return NativeLibrary.jnaMemoryLockable();
    }
  }
}
