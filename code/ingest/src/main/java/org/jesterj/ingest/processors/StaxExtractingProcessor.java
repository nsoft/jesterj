package org.jesterj.ingest.processors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.stax2.XMLInputFactory2;
import org.codehaus.stax2.XMLStreamReader2;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jesterj.ingest.trie.PatriciaTrie;

import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLResolver;
import javax.xml.stream.events.XMLEvent;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.util.*;
import java.util.regex.Pattern;

/**
 * A class for extracting fields from an xml document using a memory efficient Stax parsing.
\ */
public class StaxExtractingProcessor implements DocumentProcessor {
  static final Logger log = LogManager.getLogger();

  private String name;
  private int capacity;
  private PatriciaTrie<List<ElementSpec>> extractMapping = new PatriciaTrie<>();
  private boolean failOnLongPath = false; // default
  private XMLResolver resolver;
  private boolean supportExternalEntities;


  @Override
  public Document[] processDocument(Document document) {
    List<LimitedStaxHandler> handlers = new ArrayList<>();
    CharBuffer path = CharBuffer.allocate(this.capacity);
    path.flip();
    if (log.isTraceEnabled()) {
      log.trace(new String(document.getRawData()));
    }
    String trim = new String(document.getRawData()).trim();
    byte[] buf = trim.getBytes();
    InputStream xmlInputStream = new ByteArrayInputStream(buf);
    XMLInputFactory2 xmlInputFactory = (XMLInputFactory2) XMLInputFactory
        .newFactory("javax.xml.stream.XMLInputFactory", Thread.currentThread().getContextClassLoader());
    if (supportExternalEntities) {
      if (!xmlInputFactory.isPropertySupported("javax.xml.stream.isSupportingExternalEntities")) {
        throw new RuntimeException(xmlInputFactory + " doesn't support javax.xml.stream.isSupportingExternalEntities");
      }
      xmlInputFactory.setProperty("javax.xml.stream.isSupportingExternalEntities", true);
    }
    xmlInputFactory.setXMLResolver(resolver);
    XMLStreamReader2 xmlStreamReader;
    try {
      xmlStreamReader = (XMLStreamReader2) xmlInputFactory.createXMLStreamReader(xmlInputStream);
      while (xmlStreamReader.hasNext()) {
        int eventType = xmlStreamReader.next();
        switch (eventType) {
          case XMLEvent.START_ELEMENT:
            String s = xmlStreamReader.getName().toString();
            if (!addToPath(s, path) && failOnLongPath) {
              document.setStatus(Status.ERROR);
              log.info("Errored Document:{}",document.getId());
              return new Document[]{document};
            }
            log.trace("Starting {}", path.toString());
            List<ElementSpec> specList = extractMapping.get(path);
            if (specList != null) {
              for (ElementSpec spec : specList) {
                LimitedStaxHandler handler = spec.handleIfMatches(xmlStreamReader, spec);
                if (handler != null) {
                  log.trace("{} adding handler {} for {}", document::getId, () -> handler.getSpec().getDestField(), path::toString);
                  handlers.add(handler);
                }
              }
            }
            for (LimitedStaxHandler handler : handlers) {
              log.trace("{} start element called for {} ({})", document::getId,() ->  path, () -> handler.getSpec().getDestField());
              handler.onStartElement(xmlStreamReader);
            }
            break;
          case XMLEvent.END_ELEMENT:
            log.trace("Ending path {}", path.toString());
            List<ElementSpec> specEndingList = extractMapping.get(path);
            if (specEndingList != null) {
              for (LimitedStaxHandler handler : handlers) {
                for (ElementSpec elementSpec : specEndingList) {
                  if (handler.getSpec() == elementSpec) {
                    log.trace("{} calling onEndElement for {} ({})", document::getId, path::toString, () -> handler.getSpec().getDestField());
                    handler.onEndElement(xmlStreamReader);
                    log.trace("{} putting field {} for path {}", document::getId, elementSpec::getDestField, () -> path);
                    document.put(elementSpec.getDestField(), handler.toString());
                    handler.reset();
                  }
                }
              }
              handlers.removeIf(handler -> specEndingList.contains(handler.getSpec()));
            }
            for (LimitedStaxHandler handler : handlers) {
              log.trace("{} calling onEndElement for {} ({})", document::getId, path::toString, () -> handler.getSpec().getDestField());
              handler.onEndElement(xmlStreamReader);
            }
            decrementPath(path);
            break;
          case XMLEvent.CHARACTERS:
            for (LimitedStaxHandler handler : handlers) {
              log.trace("{} calling onCharacters for {} ({})", document::getId, path::toString, () -> handler.getSpec().getDestField());
              handler.onCharacters(xmlStreamReader);
            }
            break;
          default:
            //do nothing
            break;
        }
      }
    } catch (Throwable e) {
      document.setStatus(Status.ERROR);
      log.error("Exception Processing XML in StaxExtractingProcessor:",e);
      log.trace("Offending XML:\n{}", trim);
      log.error(e);
      if (e instanceof Error) {
        throw (Error) e;
      }
    }
    return new Document[] {document};
  }

  private void decrementPath(CharBuffer path) {
    int lastSlash = 0;
    path.rewind();
    for (int i = 0; i < path.limit(); i++) {
      if (path.charAt(i) == '/') {
        lastSlash = i;
      }
    }
    path.limit(lastSlash);
  }

  private boolean addToPath(String s, CharBuffer path) {
    s = s.replaceAll("/", "%2F"); // must encode slashes to avoid fooling decrementPath().
    int i = 0;
    boolean result = false;
    while (path.limit() < path.capacity()) {
      if (s.length() <= i) {
        result = true; // all chars were added.
        break;
      }
      if (i == 0) {
        path.position(path.limit());
        path.limit(path.limit() + 1);
        path.append('/');
      }
      path.position(path.limit());
      path.limit(path.limit() + 1);
      path.append(s.charAt(i++));
    }
    path.rewind();
    return result;
  }

  @Override
  public String getName() {
    return name;
  }

  @SuppressWarnings("WeakerAccess")
  public static class Builder extends NamedBuilder<StaxExtractingProcessor> {

    StaxExtractingProcessor obj = new StaxExtractingProcessor();

    @Override
    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public Builder withPathBuffer(int size) {
      getObj().capacity = size;
      return this;
    }

    public Builder failOnLongPath(boolean fail) {
      getObj().failOnLongPath = fail;
      return this;
    }

    public Builder extracting(String path, ElementSpec field) {
      getObj().extractMapping.computeIfAbsent(path, (f) -> new ArrayList<>()).add(field);
      return this;
    }

    /**
     * Supply a resolver that can properly locate DTD entities.
     *
     * @param resolver an implementation of {@link XMLResolver}
     * @return this builder for further configuration
     */
    public Builder withResolver(XMLResolver resolver) {
      getObj().resolver = resolver;
      return this;
    }

    public Builder isSupportingExternalEntities(boolean support) {
      getObj().supportExternalEntities = support;
      return this;
    }
    @Override
    protected StaxExtractingProcessor getObj() {
      return obj;
    }

    private void setObj(StaxExtractingProcessor obj) {
      this.obj = obj;
    }

    public StaxExtractingProcessor build() {
      StaxExtractingProcessor object = getObj();
      setObj(new StaxExtractingProcessor());
      return object;
    }

  }

  @SuppressWarnings("WeakerAccess")
  public static class Attribute {

    private String namespace;

    public String getNamespace() {
      return namespace;
    }

    private String qname;

    public String getQname() {
      return qname;
    }

    public Attribute(String key, String qname) {
      this.namespace = key;
      this.qname = qname;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      Attribute attribute = (Attribute) o;
      return Objects.equals(namespace, attribute.namespace) &&
          Objects.equals(qname, attribute.qname);
    }

    @Override
    public int hashCode() {
      return Objects.hash(namespace, qname);
    }
  }


  /**
   * An class to describe an element and what to do with it when it's encountered.
   */
  @SuppressWarnings("WeakerAccess")
  public static class ElementSpec {
    private final String destField;
    private final Set<Attribute> attrsToInclude = new LinkedHashSet<>();
    private final Map<Attribute, Pattern> attrValueMatchers = new LinkedHashMap<>();
    private final LimitedStaxHandlerFactory fact;

    public ElementSpec(String destField, LimitedStaxHandlerFactory factory) {
      this.destField = destField;
      this.fact = factory;
    }

    public ElementSpec(String destField) {
      this(destField, new DefaultHandlerFactory());
    }

    /**
     * Causes the value of the supplied attributes to be prepended to any characters from the element.
     * Attributes will be prepended in the order they are supplied to this method and separated by a single space,
     * but only once per attribute. This method should NEVER be called during document processing only
     * during configuration.
     *
     * @param attrName the attribute to append
     * @param namespaceUri the namespace for the attribute (or null for default)
     * @return this object for further configuration.
     */
    public ElementSpec inclAttributeText(String namespaceUri, String attrName) {
      attrsToInclude.add(new Attribute(namespaceUri, attrName));
      return this;
    }

    /**
     * Causes the element to only match if the value of ALL specified attributes matches the supplied regex.
     *
     * @param namespaceUri The name space for the attribute (may be null for default ns)
     * @param attrName     The qname for the attribute
     * @param pattern      The compiled regex pattern to use for matching.
     * @return this object for further configuration.
     */
    public ElementSpec matchOnAttrValue(String namespaceUri, String attrName, Pattern pattern) {
      attrValueMatchers.put(new Attribute(namespaceUri, attrName), pattern);
      return this;
    }

    /**
     * Attempt to match the current element and return a handler if successful. Note that the question of whether
     * or not the path to this element matches is already determined by this point and only the attribute
     * matching needs to be determined.
     *
     * @param reader The reader, typically during {@link javax.xml.stream.XMLStreamConstants#START_ELEMENT}
     * @param spec The specification of how to handle the element
     * @return A handler if we match null otherwise.
     */
    public LimitedStaxHandler handleIfMatches(XMLStreamReader2 reader, ElementSpec spec) {
      if (matches(reader)) {
        StringBuilder perMatchAccumulator = new StringBuilder();
        for (Attribute attr : attrsToInclude) {
          perMatchAccumulator.append(reader.getAttributeValue(attr.getNamespace(), attr.getQname()));
          perMatchAccumulator.append(" ");
        }
        return fact.newInstance(perMatchAccumulator, spec);
      } else {
        return null;
      }
    }

    protected boolean matches(XMLStreamReader2 reader) {
      for (Map.Entry<Attribute, Pattern> attributePatt : attrValueMatchers.entrySet()) {
        Attribute attr = attributePatt.getKey();
        if (reader.getAttributeIndex(attr.getNamespace(), attr.getQname()) < 0) {
          return false;
        }
        String attributeValue = reader.getAttributeValue(attr.getNamespace(), attr.getQname());
        if (!attributePatt.getValue().matcher(attributeValue).matches()) {
          return false;
        }
      }
      return true;
    }

    public String getDestField() {
      return destField;
    }
  }

  /**
   * A factory for generating handlers given an accumulator supplied on a per-match basis.
   */
  public interface LimitedStaxHandlerFactory {
    LimitedStaxHandler newInstance(StringBuilder accumulator, ElementSpec spec);
  }

  public static class DefaultHandlerFactory implements LimitedStaxHandlerFactory {

    @Override
    public LimitedStaxHandler newInstance(StringBuilder accumulator, ElementSpec spec) {
      return new LimitedStaxHandler(accumulator, spec);
    }
  }

  /**
   * A base implementation to be extended to handle the elements within the matched elements.
   * For example a &lt;person&gt; that contains &lt;firstname&gt; and &lt;lastname&gt; elements
   * that need to be combined into a single value. "Bob Smith" The default implementation is a
   * very thin wrapper on a string buffer and will simply collect all characters. All implementations
   * should produce their results via the toString method which merely reflects the value of the
   * accumulator. The most common use will be to maintain flags turning on/off capture of a selected
   * number of sub elements (i.e. just the names but not the age or sex from a &lt;person&gt; element.
   */
  @SuppressWarnings("WeakerAccess")
  public static class LimitedStaxHandler {

    protected final StringBuilder accumulator;
    private final ElementSpec spec;

    protected LimitedStaxHandler(StringBuilder accumulator, ElementSpec spec) {
      this.accumulator = accumulator;
      this.spec = spec;
    }

    protected void onCharacters(XMLStreamReader2 xmlStreamReader) {
      accumulator.append(xmlStreamReader.getText());
    }

    protected void onStartElement(XMLStreamReader2 xmlStreamReader) {
    }

    protected void onEndElement(XMLStreamReader2 xmlStreamReader) {
    }

    public String toString() {
      return accumulator.toString();
    }

    public ElementSpec getSpec() {
      return spec;
    }

    public void reset() {
      accumulator.delete(0,accumulator.length());
    }
  }

}