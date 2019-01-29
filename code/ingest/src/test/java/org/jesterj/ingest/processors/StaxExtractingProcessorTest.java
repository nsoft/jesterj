package org.jesterj.ingest.processors;

import com.copyright.easiertest.Mock;
import com.fasterxml.aalto.stax.InputFactoryImpl;
import org.bouncycastle.util.io.Streams;
import org.jesterj.ingest.model.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static com.copyright.easiertest.EasierMocks.*;
import static org.easymock.EasyMock.expect;

public class StaxExtractingProcessorTest {

  static byte[] xmlBytes;

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
            new StaxExtractingProcessor.ElementSpec("title_s"))
        .build();
    expect(mockDocument.getRawData()).andReturn(xmlBytes);
    // note that the default element spec ignores internal tags such as <italic>
    expect(mockDocument.put("title_s", "Determinants of Pair-Living in Red-Tailed Sportive Lemurs " +
        "(Lepilemur\n                    ruficaudatus)\n" +
        "                ")).andReturn(true);

    replay();
    proc.processDocument(mockDocument);
//    InputFactoryImpl
  }

}
