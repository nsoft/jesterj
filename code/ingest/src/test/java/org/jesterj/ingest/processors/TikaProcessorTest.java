package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import org.apache.tika.exception.TikaException;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.AccessControlException;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.expect;

@SuppressWarnings("ALL")
public class TikaProcessorTest {

  private static final String HTML = "<!DOCTYPE html><html><head><title>The title</title></head><body><script>scripty</script><h1>heading</h1><div>This is some body text</div></body></html>";
  private static final String XML = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><peer><child>The title</child></peer><peer>This is some body text</peer></root>";
  private static final String XML_BROKEN = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><root><peer><child>The title</peer><peer>This is some body text</peer></root>";
  private static final String XML_CONFIG=
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
          "<properties>\n" +
          "  <parsers>\n" +
          "    <parser class=\"org.apache.tika.parser.xml.XMLParser\">\n" +
          "      <mime>application/xml</mime>\n" +
          "    </parser>\n" +
          "  </parsers>\n" +
          "</properties>";
  private static final String XML_CONFIG_BAD=
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
          "<properties>\n" +
          "  <parsers>\n" +
          "    <!-- Default Parser for most things, except for 2 mime types, and never\n" +
          "         use the Executable Parser -->\n" +
          "    <parser class=\"org.apache.tika.parser.NotAParser\">\n" +
          "      <mime-exclude>image/jpeg</mime-exclude>\n" +
          "      <mime-exclude>application/pdf</mime-exclude>\n" +
          "      <parser-exclude class=\"org.apache.tika.parser.executable.ExecutableParser\"/>\n" +
          "    </parser>\n" +
          "    <!-- Use a different parser for PDF -->\n" +
          "    <parser class=\"org.apache.tika.parser.AlsoNotAParser\">\n" +
          "      <mime>application/pdf</mime>\n" +
          "    </parser>\n" +
          "    <!-- Use a different parser for XML -->\n" +
          "  </parsers>\n" +
          "  <parsers>\n" + // tika doesn't allow two of these, the funny class names appear to be ignored
          "    <parser class=\"org.apache.tika.parser.XmlParserxxxdx\">\n" +
          "      <mime>application/xml</mime>\n" +
          "    </parser>\n" +
          "  </parsers>\n" +
          "</properties>";
  @Mock
  private Document mockDocument;

  public TikaProcessorTest() {
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
  public void testHtml() {
    TikaProcessor proc = new TikaProcessor.Builder().named("foo").appendingSuffix("_tk").truncatingTextTo(-1).build();
    expect(mockDocument.getRawData()).andReturn(HTML.getBytes()).anyTimes();
    mockDocument.setRawData(aryEq("heading\nThis is some body text\n".getBytes()));
    expect(mockDocument.put("X_TIKA_Parsed_By_tk", "org.apache.tika.parser.DefaultParser")).andReturn(true);
    expect(mockDocument.put("X_TIKA_Parsed_By_Full_Set_tk", "org.apache.tika.parser.DefaultParser")).andReturn(true);
    expect(mockDocument.put("dc_title_tk", "The title")).andReturn(true);
    expect(mockDocument.put("Content_Encoding_tk", "ISO-8859-1")).andReturn(true);
    // Apparently newer versions of Tika don't produce this. However, we still get other metadata.
    // expect(mockDocument.put("title_tk", "The title")).andReturn(true);
    expect(mockDocument.put("Content_Type_tk", "text/html; charset=ISO-8859-1")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
  }

  @Test
  public void testXml() throws ParserConfigurationException, IOException, SAXException, TikaException {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);

    TikaProcessor proc = new TikaProcessor.Builder().named("foo").truncatingTextTo(20)
        .configuredWith(doc)
        .build();
    //System.out.println(new String(new byte[] {32, 32, 32, 84, 104, 101, 32, 116, 105, 116, 108, 101, 32, 84, 104, 105, 115, 32, 105, 115}));
    expect(mockDocument.getRawData()).andReturn(XML.getBytes()).anyTimes();
    mockDocument.setRawData(aryEq("   The title This is".getBytes()));
    expect(mockDocument.put("X_TIKA_Parsed_By", "org.apache.tika.parser.CompositeParser")).andReturn(true);
    expect(mockDocument.put("X_TIKA_Parsed_By_Full_Set", "org.apache.tika.parser.CompositeParser")).andReturn(true);
    expect(mockDocument.put("Content_Type", "application/xml")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
  }

  @Test
  public void testFieldDontTouchRaw() throws Exception {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);

    TikaProcessor proc = new TikaProcessor.Builder().named("foo").truncatingTextTo(20).replacingRawData(false).intoField("extracted")
        .configuredWith(doc)
        .build();
    expect(mockDocument.getRawData()).andReturn(XML.getBytes()).anyTimes();
    expect(mockDocument.put("extracted","   The title This is")).andReturn(true);
    expect(mockDocument.put("X_TIKA_Parsed_By", "org.apache.tika.parser.CompositeParser")).andReturn(true);
    expect(mockDocument.put("X_TIKA_Parsed_By_Full_Set", "org.apache.tika.parser.CompositeParser")).andReturn(true);
    expect(mockDocument.put("Content_Type", "application/xml")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
  }

  @Test(expected = TikaException.class)
  public void testBadConfig() throws ParserConfigurationException, IOException, SAXException, TikaException {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG_BAD.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);
    replay();

    new TikaProcessor.Builder().named("foo").appendingSuffix("_tk").truncatingTextTo(20)
        .configuredWith(doc)
        .build();
  }

  @Test(expected = RuntimeException.class)
  public void testBadDoc() throws ParserConfigurationException, IOException, SAXException, TikaException {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);
    TikaProcessor proc = new TikaProcessor.Builder().named("foo").appendingSuffix("_tk").truncatingTextTo(20)
        .configuredWith(doc)
        .build();
    expect(mockDocument.getRawData()).andReturn(XML_BROKEN.getBytes()).anyTimes();
    replay();
    proc.processDocument(mockDocument);
  }

  @Test
  public void testEmptyDoc() throws ParserConfigurationException, IOException, SAXException, TikaException {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);

    TikaProcessor proc = new TikaProcessor.Builder().named("foo").appendingSuffix("_tk").truncatingTextTo(20)
        .configuredWith(doc)
        .build();
    expect(mockDocument.getRawData()).andReturn(null).anyTimes();

    replay();
    proc.processDocument(mockDocument);
  }

  private DocumentBuilder getDocumentBuilder() throws ParserConfigurationException {
    DocumentBuilderFactory factory =
        DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    DocumentBuilder builder = factory.newDocumentBuilder();
    return builder;
  }

  @Test(expected = RuntimeException.class)
  public void testRandomException() throws ParserConfigurationException, IOException, SAXException, TikaException {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);

    TikaProcessor proc = new TikaProcessor.Builder().named("foo").appendingSuffix("_tk").truncatingTextTo(20)
        .configuredWith(doc)
        .build();
    expect(mockDocument.getRawData()).andThrow(new RuntimeException());

    replay();
    proc.processDocument(mockDocument);
  }

  @Test
  public void testExceptionToIgnoreFromTika() throws ParserConfigurationException, IOException, SAXException, TikaException {
    DocumentBuilder builder = getDocumentBuilder();
    ByteArrayInputStream input = new ByteArrayInputStream(XML_CONFIG.getBytes("UTF-8"));
    org.w3c.dom.Document doc = builder.parse(input);

    TikaProcessor proc = new TikaProcessor.Builder().named("foo").appendingSuffix("_tk").truncatingTextTo(20)
        .configuredWith(doc)
        .build();
    expect(mockDocument.getRawData()).andThrow(new AccessControlException("Oh no you don't!"));

    replay();
    proc.processDocument(mockDocument);
  }

}
