package org.jesterj.ingest.utils;

import org.apache.lucene.util.Version;
import org.apache.solr.schema.FieldType;
import org.junit.Before;
import org.junit.Test;
import org.w3c.dom.Document;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

public class SolrSchemaUtilTest {

  public static final String SOLR_SCHEMA_XML = "solr-schema.xml";

  private ClassSubPathResourceLoader loader;

  @Before
  public void setUp() {
    loader = new ClassSubPathResourceLoader(getClass().getClassLoader(), "solr/configsets/default/conf/");
  }

  @Test
  public void testLoadFromCP() throws Exception {
    SolrSchemaUtil solrSchemaUtil = new SolrSchemaUtil();
    Document schemaDocument = solrSchemaUtil.getSchemaDocument(SOLR_SCHEMA_XML, loader);
    assertNotNull(schemaDocument);
  }

  @Test
  public void testLoadPrimitiveType() throws Exception {
    SolrSchemaUtil solrSchemaUtil = new SolrSchemaUtil();
    Document schemaDocument = solrSchemaUtil.getSchemaDocument(SOLR_SCHEMA_XML, loader);
    assertNotNull(schemaDocument);

    // note we want to avoid any examples with substitution because substitution is not supported.
    FieldType anInt = solrSchemaUtil.getFieldType(schemaDocument, "pint", Version.LUCENE_7_6_0.toString(), 1.0f, loader);
    assertNotNull(anInt);
    assertEquals("pint", anInt.getTypeName());
    assertEquals(anInt.getNamedPropertyValues(false).get("docValues"), true);
  }

  @Test
  public void testLoadSingleAnalyzerType() throws Exception {
    SolrSchemaUtil solrSchemaUtil = new SolrSchemaUtil();
    Document schemaDocument = solrSchemaUtil.getSchemaDocument(SOLR_SCHEMA_XML, loader);
    assertNotNull(schemaDocument);

    // note we want to avoid any examples with substitution because substitution is not supported.
    FieldType textField = solrSchemaUtil.getFieldType(schemaDocument, "text", Version.LUCENE_7_6_0.toString(), 1.0f, loader);
    assertNotNull(textField);
    assertEquals("text", textField.getTypeName());
    assertNotNull(textField.getIndexAnalyzer());
    assertEquals(textField.getIndexAnalyzer(), textField.getQueryAnalyzer());
  }

  @Test
  public void testLoadDualAnalyzerType() throws Exception {
    SolrSchemaUtil solrSchemaUtil = new SolrSchemaUtil();
    Document schemaDocument = solrSchemaUtil.getSchemaDocument(SOLR_SCHEMA_XML, loader);
    assertNotNull(schemaDocument);

    // note we want to avoid any examples with substitution because substitution is not supported.
    FieldType textField = solrSchemaUtil.getFieldType(schemaDocument, "teststop", Version.LUCENE_7_6_0.toString(), 1.0f, loader);
    assertNotNull(textField);
    assertEquals("teststop", textField.getTypeName());
    assertNotNull(textField.getIndexAnalyzer());
    assertNotNull(textField.getQueryAnalyzer());
    assertNotSame(textField.getIndexAnalyzer(), textField.getQueryAnalyzer());
  }
}
