package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import org.bouncycastle.util.io.Streams;
import org.codehaus.stax2.XMLStreamReader2;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.processors.StaxExtractingProcessor.ElementSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.xml.stream.XMLResolver;
import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class StaxExtractingProcessorTest {

  private static byte[] xmlBytes;

  @Mock private Document mockDocument;
  @Mock private XMLResolver mockResolver;

  public StaxExtractingProcessorTest() {
    prepareMocks(this);
  }

  @BeforeClass
  public static void init() throws IOException {
    InputStream resourceAsStream = StaxExtractingProcessor.class.getResourceAsStream("/pubmed.xml");
    xmlBytes = Streams.readAll(resourceAsStream);
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
  public void testSimpleSingleValue() {
    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testSimpleSingleValue")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .extracting("/article/front/article-meta/title-group/article-title",
            new ElementSpec("title_s"))
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    // also note that whitespace is not collapsed, this is not html.
    expect(mockDocument.put("title_s", "Determinants of Pair-Living in Red-Tailed Sportive Lemurs " +
        "(Lepilemur\n                    ruficaudatus)\n                ")).andReturn(true);

    replay();
    Document[] documents = proc.processDocument(mockDocument);
    assertEquals(1, documents.length);
    assertEquals(mockDocument,documents[0]);
  }

  @Test
  public void testSimpleMultipleValue() {
    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testSimpleMultipleValue")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .extracting("/article/front/journal-meta/journal-id",
            new ElementSpec("journal_id_s"))
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    expect(mockDocument.put("journal_id_s", "Ethology")).andReturn(true);
    expect(mockDocument.put("journal_id_s", "Ethology")).andReturn(true);
    expect(mockDocument.put("journal_id_s", "eth")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
  }

  @Test
  public void testSimpleFilterByAttribute() {
    Pattern nlmta = Pattern.compile("nlm-ta");
    ElementSpec journal_id_s = new ElementSpec("journal_id_s");
    journal_id_s.matchOnAttrValue(null,"journal-id-type", nlmta);
    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testSimpleFilterByAttribute")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .extracting("/article/front/journal-meta/journal-id", journal_id_s)
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    expect(mockDocument.put("journal_id_s", "Ethology")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);

  }

  @Test
  public void testIncludeAttributeText() {
    Pattern nlmta = Pattern.compile("nlm-ta");
    ElementSpec journal_id_s = new ElementSpec("journal_id_s");
    journal_id_s.matchOnAttrValue(null,"journal-id-type", nlmta);
    journal_id_s.inclAttributeText(null, "journal-id-type");
    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testIncludeAttributeText")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .extracting("/article/front/journal-meta/journal-id", journal_id_s)
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    expect(mockDocument.put("journal_id_s", "nlm-ta Ethology")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);

  }

  @Test
  public void testExtractSamePathToMultipleFieldsByAttribute() {
    Pattern nlmta = Pattern.compile("nlm-ta");
    Pattern iso = Pattern.compile("iso-abbrev");
    Pattern pub = Pattern.compile("publisher-id");

    ElementSpec journal_nlm_ta = new ElementSpec("journal_nlm_ta");
    journal_nlm_ta.matchOnAttrValue(null,"journal-id-type", nlmta);
    ElementSpec journal_iso = new ElementSpec("journal_iso");
    journal_iso.matchOnAttrValue(null,"journal-id-type", iso);
    ElementSpec journal_pub = new ElementSpec("journal_pub");
    journal_pub.matchOnAttrValue(null,"journal-id-type", pub);
    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testExtractSamePathToMultipleFieldsByAttribute")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .extracting("/article/front/journal-meta/journal-id", journal_nlm_ta)
        .extracting("/article/front/journal-meta/journal-id", journal_iso)
        .extracting("/article/front/journal-meta/journal-id", journal_pub)
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    expect(mockDocument.put("journal_nlm_ta", "Ethology")).andReturn(true);
    expect(mockDocument.put("journal_iso", "Ethology")).andReturn(true);
    expect(mockDocument.put("journal_pub", "eth")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
  }

  @Test
  public void testCustomHandler() {
    Pattern nlmta = Pattern.compile("author");
    ElementSpec author = new ElementSpec("author_s", (accumulator, spec) ->
        new StaxExtractingProcessor.LimitedStaxHandler(accumulator,spec) {
      StringBuilder surname = new StringBuilder();
      StringBuilder givenName = new StringBuilder();
      boolean inSurname = false;
      boolean inGivenName = false;

      @Override
      protected void onCharacters(XMLStreamReader2 xmlStreamReader) {
        if(inSurname) {
          surname.append(xmlStreamReader.getText());
        }
        if(inGivenName) {
          givenName.append(xmlStreamReader.getText());
        }
      }

      @Override
      protected void onStartElement(XMLStreamReader2 xmlStreamReader) {
        if ("surname".equals(xmlStreamReader.getName().getLocalPart())) {
          inSurname = true;
        }
        if ("given-names".equals(xmlStreamReader.getName().getLocalPart())) {
          inGivenName = true;
        }
      }

      @Override
      protected void onEndElement(XMLStreamReader2 xmlStreamReader) {
        if(inSurname && "surname".equals(xmlStreamReader.getName().getLocalPart())) {
          surname.append(" ");
          inSurname  = false;
        }
        if(inGivenName && "given-names".equals(xmlStreamReader.getName().getLocalPart())) {
          givenName.append(" ");
          inGivenName = false;
        }
      }

      @Override
      public String toString() {
        return (givenName.toString() + surname.toString()).trim();
      }
    }) ;
    author.matchOnAttrValue(null,"contrib-type", nlmta);

    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testCustomHandler")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .extracting("/article/front/article-meta/contrib-group/contrib", author)
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    expect(mockDocument.put("author_s", "Roland Hilgartner")).andReturn(true);
    expect(mockDocument.put("author_s", "Claudia Fichtel")).andReturn(true);
    expect(mockDocument.put("author_s", "Peter M Kappeler")).andReturn(true);
    expect(mockDocument.put("author_s", "Dietmar Zinner")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);

  }

  @Test
  public void testResolver() {
    StaxExtractingProcessor proc = new StaxExtractingProcessor.Builder()
        .named("testSimpleSingleValue")
        .failOnLongPath(true)
        .withPathBuffer(2048)
        .isSupportingExternalEntities(true)
        .withResolver(mockResolver)
        .extracting("/article/front/article-meta/title-group/article-title",
            new ElementSpec("title_s"))
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    // also note that whitespace is not collapsed, this is not html.
    expect(mockDocument.put("title_s", "Determinants of Pair-Living in Red-Tailed Sportive Lemurs " +
        "(Lepilemur\n                    ruficaudatus)\n                ")).andReturn(true);

    replay();
    Document[] documents = proc.processDocument(mockDocument);
    assertEquals(1, documents.length);
    assertEquals(mockDocument,documents[0]);
  }
}
