package org.jesterj.ingest.processors;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;
import org.apache.solr.schema.FieldType;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.utils.ClassSubPathResourceLoader;
import org.jesterj.ingest.utils.SolrSchemaUtil;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Supplier;


/**
 * A processor that will produce a json value representing the analysis for the field. This value is
 * suitable for indexing into fields configured with {@link org.apache.solr.schema.PreAnalyzedField}.
 * Obviously the field type specified here can't resolve to PreAnalyzedField or we are in a loop, and
 * the field type definitions that will work are limited to ones without substitutuion, Also, anything
 * that would involve access to a BlobStore, SolrCore, CoreContainer or SolrResourceLoader will fail
 * (because we are not actually running inside solr). Those caveats aside, it should consume any normal
 * field type definition from the supplied schema and use that definition to produce the pre-analyzed JSON.
 */
public class PreAnalyzeFields implements DocumentProcessor {
  @SuppressWarnings("unused")
  private static final Logger log = LogManager.getLogger();

  private ThreadLocal<Analyzer> analyzer = new ThreadLocal<>() {
    @Override
    protected Analyzer initialValue() {
      try {
        return analyzerFactory.call();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  };

  private Supplier<ClassLoader> classLoaderProvider = new Supplier<>() {
    @Override
    public ClassLoader get() {
      return this.getClass().getClassLoader();
    }
  };
  private ClassSubPathResourceLoader loader;
  private Callable<Analyzer> analyzerFactory;
  private String name;
  private List<String> fieldsToAnalyze = new ArrayList<>();
  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public Document[] processDocument(Document document) {
    for (String docFieldName : fieldsToAnalyze) {
      List<String> values = document.get(docFieldName);
      List<String> jsonValues = new ArrayList<>(values.size());
      for (String value : values) {
        Map<String, Object> tokOutput = new HashMap<>();
        tokOutput.put("v", "1");
        tokOutput.put("str", value);
        ArrayList<Map<String, Object>> tokens = new ArrayList<>();
        tokOutput.put("tokens", tokens);
        TokenStream ts = analyzer.get().tokenStream(docFieldName, value);
        try {
          ts.reset();
          OffsetAttribute offsetA = ts.getAttribute(OffsetAttribute.class);
          CharTermAttribute charTermA = ts.getAttribute(CharTermAttribute.class);
          PositionIncrementAttribute posIncA = ts.getAttribute(PositionIncrementAttribute.class);
          PayloadAttribute payloadA = ts.getAttribute(PayloadAttribute.class);
          TypeAttribute typeA = ts.getAttribute(TypeAttribute.class);
          FlagsAttribute flagsA = ts.getAttribute(FlagsAttribute.class);
          while (ts.incrementToken()) {
            Map<String, Object> tokAttrs = new HashMap<>();
            char[] termChars = new char[charTermA.length()];
            System.arraycopy(charTermA.buffer(),0,termChars,0,charTermA.length());
            tokAttrs.put("t", new String(termChars));
            tokAttrs.put("s", offsetA.startOffset());
            tokAttrs.put("e", offsetA.endOffset());
            tokAttrs.put("i", posIncA.getPositionIncrement());
            Base64.Encoder encoder = Base64.getEncoder();
            if (payloadA != null) {
              BytesRef payload = payloadA.getPayload();
              if (payload != null) {
                tokAttrs.put("p", encoder.encode(payload.bytes));
              }
            }
            tokAttrs.put("y", typeA.type());
            if (flagsA != null) {
              // Solr uses Integer.parseInt(String.valueOf(e.getValue()), 16) so toHexString() doesn't work.
              tokAttrs.put("f", Integer.toString(flagsA.getFlags(), 16));
            }
            tokens.add(tokAttrs);
          }
          ts.end();
          jsonValues.add(mapper.writeValueAsString(tokOutput));
        } catch (IOException e) {
          document.setStatus(Status.ERROR);
          throw new RuntimeException(e);
        }
      }
      document.removeAll(docFieldName);
      document.putAll(docFieldName, jsonValues);
    }
    return new Document[]{document};
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<PreAnalyzeFields> {

    PreAnalyzeFields obj = new PreAnalyzeFields();
    private String typeName;
    @SuppressWarnings("deprecation")
    private String luceneMatch = Version.LUCENE_7_6_0.toString(); // default
    private String schemaFile = "schema.xml"; // default
    private float schemaVersion;

    @Override
    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public Builder preAnalyzingField(String field) {
      getObj().fieldsToAnalyze.add(field);
      return this;
    }

    public Builder forTypeNamed(String name) {
      this.typeName = name;
      return this;
    }

    public Builder withLuceneMatchVersion(String version) {
      this.luceneMatch = version;
      return this;
    }

    public Builder loadingResourcesVia(Supplier<ClassLoader> provider ) {
      getObj().classLoaderProvider = provider;
      return this;
    }

    /**
     * Set the file in the classpath for the schema file. The file will be loaded from the top
     * of the class path (as if / is pre-pended), but may begin with / and any amount of sub-paths
     * resources such as stopwords.txt are loaded relative to the position of this file in the class
     * path.
     *
     * @param filename the name of the file, usually schema.xml or managed-schema
     * @return this builder for further configuration
     */
    //FUTURE: might be nice to accept a zk:// url too
    public Builder fromFile(String filename) {
      this.schemaFile = filename;
      return this;
    }

    public Builder withSchemaVersion(float version) {
      this.schemaVersion = version;
      return this;
    }


    @Override
    protected PreAnalyzeFields getObj() {
      return obj;
    }


    private void setObj(PreAnalyzeFields obj) {
      this.obj = obj;
    }


    public PreAnalyzeFields build() {
      final SolrSchemaUtil util = new SolrSchemaUtil();

      try {
        int endIndex = schemaFile.lastIndexOf("/");
        String subpath;
        if (endIndex > 0) {
          subpath = schemaFile.substring(0, endIndex + 1);
          schemaFile = schemaFile.substring(endIndex + 1);
        } else {
          subpath = "";
        }
        obj.loader = new ClassSubPathResourceLoader(obj.classLoaderProvider.get(), subpath);
        org.w3c.dom.Document doc = util.getSchemaDocument(schemaFile, obj.loader);
        FieldType ft = util.getFieldType(doc, typeName, luceneMatch, schemaVersion, obj.loader);
        obj.analyzerFactory = ft::getIndexAnalyzer;
      } catch (IllegalAccessException | InstantiationException | ParserConfigurationException | IOException | XPathExpressionException | SAXException e) {
        throw new RuntimeException(e);
      }

      PreAnalyzeFields built = getObj();
      setObj(new PreAnalyzeFields());
      return built;
    }


  }


}



