package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import org.bouncycastle.util.io.Streams;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.processors.StaxExtractingProcessor.ElementSpec;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.regex.Pattern;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.expect;

public class StaxExtractingProcessorTest {

  private static byte[] xmlBytes;

  @Mock
  private Document mockDocument;

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
    expect(mockDocument.put("title_s", "Determinants of Pair-Living in Red-Tailed Sportive Lemurs " +
        "(Lepilemur\n                    ruficaudatus)\n" +
        "                ")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
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
}
