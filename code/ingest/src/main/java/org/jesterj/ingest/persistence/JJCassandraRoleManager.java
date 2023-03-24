package org.jesterj.ingest.persistence;

import org.apache.cassandra.auth.AuthKeyspace;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.service.StorageService;
import org.apache.commons.lang3.StringUtils;
import org.mindrot.jbcrypt.BCrypt;

public class JJCassandraRoleManager extends CassandraRoleManager {

  private final String defaultSuperuserPassword;

  public JJCassandraRoleManager(String pw) {
    defaultSuperuserPassword = pw;
  }

  @Override
  public void setup() {
    loadRoleStatement();
    scheduleSetupTask(() -> {
      setupDefaultRoleJJ();
      return null;
    });  }

  private void setupDefaultRoleJJ() {

    if (StorageService.instance.getTokenMetadata().sortedTokens().isEmpty())
      throw new IllegalStateException("CassandraRoleManager skipped default role setup: no known tokens in ring");

    if (!hasExistingRoles()) {
      QueryProcessor.process(createJJDefaultRoleQuery(),
          consistencyForRoleWrite(DEFAULT_SUPERUSER_NAME));
    }

  }

  private String createJJDefaultRoleQuery() {
    return String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', true, true, '%s') USING TIMESTAMP 0",
        SchemaConstants.AUTH_KEYSPACE_NAME,
        AuthKeyspace.ROLES,
        DEFAULT_SUPERUSER_NAME,
        escape(hashpw(defaultSuperuserPassword)));
  }

  private static String escape(String name)
  {
    return StringUtils.replace(name, "'", "''");
  }

  private static String hashpw(String password)
  {
    return BCrypt.hashpw(password, BCrypt.gensalt(GENSALT_LOG2_ROUNDS));
  }

  private static final int GENSALT_LOG2_ROUNDS = getGensaltLogRounds();

  static int getGensaltLogRounds()
  {
    int rounds = Integer.getInteger(GENSALT_LOG2_ROUNDS_PROPERTY, 10);
    if (rounds < 4 || rounds > 30)
      throw new ConfigurationException(String.format("Bad value for system property -D%s." +
              "Please use a value between 4 and 30 inclusively",
          GENSALT_LOG2_ROUNDS_PROPERTY));
    return rounds;
  }
}
