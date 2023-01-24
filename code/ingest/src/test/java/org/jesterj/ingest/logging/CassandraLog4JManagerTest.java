package org.jesterj.ingest.logging;

import com.copyright.easiertest.Mock;
import com.copyright.easiertest.ObjectUnderTest;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Optional;
import java.util.concurrent.CompletionStage;

import static com.copyright.easiertest.EasierMocks.prepareMocks;
import static com.copyright.easiertest.EasierMocks.replay;
import static com.copyright.easiertest.EasierMocks.reset;
import static com.copyright.easiertest.EasierMocks.verify;
import static org.easymock.EasyMock.expect;
import static org.jesterj.ingest.logging.CassandraLog4JManager.*;

public class CassandraLog4JManagerTest {
  @ObjectUnderTest CassandraLog4JManager manager;
  @Mock private CqlSession sessionMock;
  @Mock private CompletionStage<Metadata> csMock;
  @Mock private Metadata metaMock;
  @Mock private KeyspaceMetadata ksMock;
  @Mock private TableMetadata tableMock;
  @Mock private ResultSet rsMock;
  @Mock private ColumnMetadata colMock;

  public CassandraLog4JManagerTest() {
    prepareMocks(this);
  }

  @Before
  public void setUp() {
    reset();
  }

  @After
  public void tearDown() {
    verify();
  }

  @Test
  public void testEnsureBasicSchema() {
    expect(sessionMock.setSchemaMetadataEnabled(false)).andReturn(csMock);
    expect(sessionMock.execute(CREATE_LOG_KEYSPACE)).andReturn(null);
    expect(sessionMock.execute(CREATE_LOG_TABLE)).andReturn(null);
    expect(sessionMock.execute(CREATE_FT_TABLE)).andReturn(null);
    expect(sessionMock.execute(FTI_STATUS_INDEX)).andReturn(null);
    expect(sessionMock.execute(FTI_SCANNER_INDEX)).andReturn(null);
    expect(sessionMock.execute(CREATE_FT_KEYSPACE)).andReturn(null);
    expect(sessionMock.execute(CREATE_NEW_FT_TABLE)).andReturn(null);
    expect(sessionMock.execute(INDEX_STATUS)).andReturn(null);
    expect(sessionMock.setSchemaMetadataEnabled(true)).andReturn(csMock);
    expect(sessionMock.checkSchemaAgreement()).andReturn(true);

    replay();
    manager.ensureBasicSchema(sessionMock);
  }

  @Test
  public void testUpgradeAddColIfMissingNoKeyspace() {
    expect(sessionMock.getMetadata()).andReturn(metaMock);
    expect(metaMock.getKeyspace("jj_logging")).andReturn(Optional.empty());
    replay();
    manager.upgradeAddColIfMissing(sessionMock, "foo", "bar", "int");
  }

  @Test
  public void testUpgradeAddColIfMissingNoTable() {
    expect(sessionMock.getMetadata()).andReturn(metaMock);
    expect(metaMock.getKeyspace("jj_logging")).andReturn(Optional.of(ksMock));
    expect(ksMock.getTable("foo")).andReturn(Optional.empty());
    replay();
    manager.upgradeAddColIfMissing(sessionMock, "foo", "bar", "int");
  }

  @Test
  public void testUpgradeAddColIfMissingNoCol() {
    expect(sessionMock.getMetadata()).andReturn(metaMock);
    expect(metaMock.getKeyspace("jj_logging")).andReturn(Optional.of(ksMock));
    expect(ksMock.getTable("foo")).andReturn(Optional.of(tableMock));
    expect(tableMock.getColumn("bar")).andReturn(Optional.empty());
    expect(sessionMock.execute("ALTER TABLE jj_logging.foo ADD bar int")).andReturn(rsMock);
    expect(sessionMock.checkSchemaAgreement()).andReturn(true);
    replay();
    manager.upgradeAddColIfMissing(sessionMock, "foo", "bar", "int");
  }
  @Test
  public void testUpgradeAddColIfMissingColExists() {
    expect(sessionMock.getMetadata()).andReturn(metaMock);
    expect(metaMock.getKeyspace("jj_logging")).andReturn(Optional.of(ksMock));
    expect(ksMock.getTable("foo")).andReturn(Optional.of(tableMock));
    expect(tableMock.getColumn("bar")).andReturn(Optional.of(colMock));
    replay();
    manager.upgradeAddColIfMissing(sessionMock, "foo", "bar", "int");
  }
}
