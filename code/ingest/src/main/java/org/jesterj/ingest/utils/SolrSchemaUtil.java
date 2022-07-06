/*
 * Copyright 2016 Needham Software LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*

Notice: Substantial portions of this code are lifted from version 7.6 of Apache Solr and used
via the Apache 2.0 License. Changes from original are noted in the code below

 */
package org.jesterj.ingest.utils;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharFilterFactory;
import org.apache.lucene.analysis.TokenFilterFactory;
import org.apache.lucene.analysis.TokenizerFactory;
import org.apache.lucene.analysis.core.KeywordAnalyzer;
import org.apache.lucene.util.ResourceLoader;
import org.apache.lucene.util.ResourceLoaderAware;
import org.apache.lucene.util.Version;
import org.apache.solr.analysis.TokenizerChain;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.DOMUtil;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.HasImplicitIndexAnalyzer;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.TextField;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.solr.common.params.CommonParams.NAME;
import static org.apache.solr.schema.IndexSchema.FIELD_TYPE;
import static org.apache.solr.schema.IndexSchema.LUCENE_MATCH_VERSION_PARAM;
import static org.apache.solr.schema.IndexSchema.SCHEMA;
import static org.apache.solr.schema.IndexSchema.TYPES;
import static org.eclipse.jetty.util.URIUtil.SLASH;

/**
 * A stateful utility bean for working with schema documents. This class is not thread safe.
 */
@SuppressWarnings("rawtypes")
public class SolrSchemaUtil {
  private static final Logger log = LogManager.getLogger();

  private static final Class[] NO_CLASSES = new Class[0];
  private static final Object[] NO_OBJECTS = new Object[0];


  private static final String XPATH_OR = " | ";
  private static final String[] NO_SUB_PACKAGES = new String[0];
  public static final String SCHEMA_XML_ANALYZER_FILTER = "[schema.xml] analyzer/filter";
  public static final String SCHEMA_XML_ANALYZER_TOKENIZER = "[schema.xml] analyzer/tokenizer";
  public static final String SCHEMA_XML_ANALYZER_CHAR_FILTER = "[schema.xml] analyzer/charFilter";

  //*** These fields below copied verbatim from Solr 7.6's SolrResourceLoader

  static final String project = "solr";
  static final String base = "org.apache" + "." + project;
  static final String[] packages = {
      "", "analysis.", "schema.", "handler.", "handler.tagger.", "search.", "update.", "core.", "response.", "request.",
      "update.processor.", "util.", "spelling.", "handler.component.", "handler.dataimport.",
      "spelling.suggest.", "spelling.suggest.fst.", "rest.schema.analysis.", "security.", "handler.admin.",
      "cloud.autoscaling."
  };

  // Using this pattern, legacy analysis components from previous Solr versions are identified and delegated to SPI loader:
  private static final Pattern legacyAnalysisPattern =
      Pattern.compile("((\\Q" + base + ".analysis.\\E)|(\\Q" + project + ".\\E))([\\p{L}_$][\\p{L}\\p{N}_$]+?)(TokenFilter|Filter|Tokenizer|CharFilter)Factory");

  /*
   * A static map of short class name to fully qualified class name
   */
  private static final Map<String, String> classNameCache = new ConcurrentHashMap<>();

  //*** end verbatim copy

  private final XPath xpath = XPathFactory.newInstance().newXPath();

  public Document getSchemaDocument(String schemaFile, ClassSubPathResourceLoader classLoader) throws ParserConfigurationException, SAXException, IOException {
    InputStream resourceAsStream = classLoader.openResource(schemaFile);
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    DocumentBuilder db = dbf.newDocumentBuilder();
    return db.parse(resourceAsStream);
  }

  private static <T> T create(final Class<T> classToMock) {
    Objenesis objenesis = new ObjenesisStd();
    return objenesis.getInstantiatorOf(classToMock).newInstance();
  }

  public FieldType getFieldType(Document doc, String fieldTypeName, String luceneMatch, float schemaVersion, ClassSubPathResourceLoader loader) throws XPathExpressionException, InstantiationException, IllegalAccessException {
    FieldType ft = null;
    Node node = null;
    String expression = getFieldTypeXPathExpressions();
    NodeList nodeList = (NodeList) xpath.evaluate(expression, doc, XPathConstants.NODESET);
    for (int k = 0; k < nodeList.getLength(); k++) {
      Node n = nodeList.item(k);
      String aName = n.getAttributes().getNamedItem("name").getNodeValue();
      if (fieldTypeName.equals(aName)) {
        node = n;
        String aClass = n.getAttributes().getNamedItem("class").getNodeValue();
        Class clazz = findClass(aClass, Object.class);
        //noinspection deprecation
        ft = (FieldType) clazz.newInstance();
        break;
      }
    }

    if (ft == null) {
      throw new RuntimeException("Could not find field type " + fieldTypeName);
    }

    commitHorribleCrimesAgainstTypeSafety(fieldTypeName, schemaVersion, ft, node);

    // from here copied from org.apache.solr.schema.FieldTypePluginLoader.create with changes to avoid needing
    // a SolrResourceLoader and omissions noted below
    expression = "./analyzer[@type='query']";
    Node anode = (Node) xpath.evaluate(expression, node, XPathConstants.NODE);
    Analyzer queryAnalyzer = readAnalyzer(anode, luceneMatch, loader);

    expression = "./analyzer[@type='multiterm']";
    anode = (Node) xpath.evaluate(expression, node, XPathConstants.NODE);
    Analyzer multiAnalyzer = readAnalyzer(anode, luceneMatch, loader);

    // An analyzer without a type specified, or with type="index"
    expression = "./analyzer[not(@type)] | ./analyzer[@type='index']";
    anode = (Node) xpath.evaluate(expression, node, XPathConstants.NODE);
    Analyzer analyzer = readAnalyzer(anode, luceneMatch, loader);

    // CHANGE vs. Solr: removed loading of similarity because we are only worried about indexing and
    // skipping it saves us having to deal with IndexSchema#readSimilarity(), which wants a
    // SolrResourceLoader that we don't have.

    if (ft instanceof HasImplicitIndexAnalyzer) {
      ft.setIsExplicitAnalyzer(false);
      if (null != queryAnalyzer && null != analyzer) {
        if (log.isWarnEnabled()) {
          log.warn("Ignoring index-time analyzer for type: " + fieldTypeName);
        }
      } else if (null == queryAnalyzer) { // Accept non-query-time analyzer as a query-time analyzer
        queryAnalyzer = analyzer;
      }
      if (null != queryAnalyzer) {
        ft.setIsExplicitQueryAnalyzer(true);
        ft.setQueryAnalyzer(queryAnalyzer);
      }
    } else {
      if (null == queryAnalyzer) {
        queryAnalyzer = analyzer;
        ft.setIsExplicitQueryAnalyzer(false);
      } else {
        ft.setIsExplicitQueryAnalyzer(true);
      }
      if (null == analyzer) {
        analyzer = queryAnalyzer;
        ft.setIsExplicitAnalyzer(false);
      } else {
        ft.setIsExplicitAnalyzer(true);
      }

      if (null != analyzer) {
        ft.setIndexAnalyzer(analyzer);
        ft.setQueryAnalyzer(queryAnalyzer);
        if (ft instanceof TextField) {
          if (null == multiAnalyzer) {
            multiAnalyzer = constructMultiTermAnalyzer(queryAnalyzer);
            ((TextField) ft).setIsExplicitMultiTermAnalyzer(false);
          } else {
            ((TextField) ft).setIsExplicitMultiTermAnalyzer(true);
          }
          ((TextField) ft).setMultiTermAnalyzer(multiAnalyzer);
        }
      }
    }
    return ft;
  }

  private void commitHorribleCrimesAgainstTypeSafety(String fieldTypeName, float schemaVersion, FieldType ft, Node node) throws IllegalAccessException {
    boolean found = false;
    Class clazz = ft.getClass();
    do {
      try {
        // mumble mumble package private setter, mumble mumble.
        Field typeName = clazz.getDeclaredField("typeName");
        found = true;
        typeName.setAccessible(true);
        typeName.set(ft, fieldTypeName);
      } catch (NoSuchFieldException e) {
        clazz = clazz.getSuperclass();
      }
    } while (!found && clazz != Object.class);

    if (!found) {
      throw new RuntimeException("Couldn't find fieldType field to set it to" + fieldTypeName);
    }

    Map<String, String> map = DOMUtil.toMapExcept(node.getAttributes(), NAME);
    IndexSchema indexSchema = create(IndexSchema.class);
    try {
      Field versionField = IndexSchema.class.getDeclaredField("version");
      versionField.setAccessible(true);
      versionField.set(indexSchema, schemaVersion);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    }

    Method setArgs;
    try {
      setArgs = FieldType.class.getDeclaredMethod("setArgs", IndexSchema.class, Map.class);
      setArgs.setAccessible(true);
      setArgs.invoke(ft, indexSchema, map);
    } catch (NoSuchMethodException | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read an analyzer from a dom node. This is adapted from {@link org.apache.solr.schema.FieldTypePluginLoader} with
   * changes to avoid requiring a SolrResourceLoader.
   *
   * @param node        The dom node representing the analyzer
   * @param luceneMatch The lucene version match (must be supplied since we don't load a SolrConfig.xml)
   * @param loader      The Resource loader that can provide accessory files such as stopwords.txt
   * @return A freshly instantiated analyzer
   * @throws XPathExpressionException if there are problems with the DOM created from the schema.xml file.
   */
  private Analyzer readAnalyzer(Node node, final String luceneMatch, ResourceLoader loader) throws XPathExpressionException {


    // parent node used to be passed in as "fieldtype"
    // if (!fieldtype.hasChildNodes()) return null;
    // Node node = DOMUtil.getChild(fieldtype,"analyzer");

    if (node == null) return null;
    NamedNodeMap attrs = node.getAttributes();
    String analyzerClassName = DOMUtil.getAttr(attrs, "class");

    // check for all of these up front, so we can error if used in
    // conjunction with an explicit analyzer class.
    NodeList charFilterNodes = (NodeList) xpath.evaluate
        ("./charFilter", node, XPathConstants.NODESET);
    NodeList tokenizerNodes = (NodeList) xpath.evaluate
        ("./tokenizer", node, XPathConstants.NODESET);
    NodeList tokenFilterNodes = (NodeList) xpath.evaluate
        ("./filter", node, XPathConstants.NODESET);

    if (analyzerClassName != null) {

      // explicitly check for child analysis factories instead of
      // just any child nodes, because the user might have their
      // own custom nodes (ie: <description> or something like that)
      if (0 != charFilterNodes.getLength() ||
          0 != tokenizerNodes.getLength() ||
          0 != tokenFilterNodes.getLength()) {
        throw new SolrException
            (SolrException.ErrorCode.SERVER_ERROR,
                "Configuration Error: Analyzer class='" + analyzerClassName +
                    "' can not be combined with nested analysis factories");
      }

      try {
        // No need to be core-aware as Analyzers are not in the core-aware list
        final Class<? extends Analyzer> clazz = findClass(analyzerClassName, Analyzer.class);
        Analyzer analyzer = clazz.getDeclaredConstructor().newInstance();

        final String matchVersionStr = DOMUtil.getAttr(attrs, LUCENE_MATCH_VERSION_PARAM);
        final Version luceneMatchVersion = (matchVersionStr == null) ?
            Version.parse(luceneMatch) :
            SolrConfig.parseLuceneVersionString(matchVersionStr);
        if (luceneMatchVersion == null) {
          throw new SolrException
              (SolrException.ErrorCode.SERVER_ERROR,
                  "Configuration Error: Analyzer '" + clazz.getName() +
                      "' needs a 'luceneMatchVersion' parameter");
        }
        // Not required see LUCENE-9545
        // analyzer.setVersion(luceneMatchVersion);
        if (analyzer instanceof ResourceLoaderAware) {
          ((ResourceLoaderAware) analyzer).inform(loader);
        }
        return analyzer;
      } catch (Exception e) {
        log.error("Cannot load analyzer: " + analyzerClassName, e);
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
            "Cannot load analyzer: " + analyzerClassName, e);
      }
    }

    // Load the CharFilters

    final ArrayList<CharFilterFactory> charFilters = new ArrayList<>();
    load(charFilterNodes, SCHEMA_XML_ANALYZER_CHAR_FILTER, charFilters, CharFilterFactory.class, luceneMatch, loader);

    // Load the Tokenizer
    // Although an analyzer only allows a single Tokenizer, we load a list to make sure
    // the configuration is ok

    final ArrayList<TokenizerFactory> tokenizers = new ArrayList<>(1);
    load(tokenizerNodes, SCHEMA_XML_ANALYZER_TOKENIZER, tokenizers, TokenizerFactory.class, luceneMatch, loader);

    // Make sure something was loaded
    if (tokenizers.isEmpty()) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "analyzer without class or tokenizer");
    }

    // Load the Filters

    final ArrayList<TokenFilterFactory> filters = new ArrayList<>();
    load(tokenFilterNodes, SCHEMA_XML_ANALYZER_FILTER, filters, TokenFilterFactory.class, luceneMatch, loader);

    return new TokenizerChain(charFilters.toArray(new CharFilterFactory[0]),
        tokenizers.get(0), filters.toArray(new TokenFilterFactory[0]));

  }

  /**
   * Initializes and each plugin in the list.
   * Given a NodeList from XML in the form:
   * <pre class="prettyprint">
   * {@code
   * <plugins>
   *    <plugin name="name1" class="solr.ClassName" >
   *      ...
   *    </plugin>
   *    <plugin name="name2" class="solr.ClassName" >
   *      ...
   *    </plugin>
   * </plugins>}
   * </pre>
   * <p>
   * This will attempt to initialize each plugin from the list. Plugins with factories that
   * pull additional information from a SolrResourceLoader or with constructors requiring parameters may not work
   * unless that logic is pulled into this class. As of this writing this has only been done for TokenFilterFactory,
   * TokenizerFactory and CharFilterFactory. This is similar to SolrResourceLoader but contains class specific
   * logic, skips all class registration and does not perform default checking.
   *
   * @param <T>         the generic type of the plugins to load
   * @param nodes       the nodelist specifying the plugin.
   * @param errRef      a string to include with error messages
   * @param list        a list to which loaded plugin objects will be added
   * @param clazz       a type for which the loaded plugins should have the same class or be a subclass of
   * @param luceneMatch the lucene match version to be supplied to plugins that care about it.
   * @param loader      the resource loader with which to load additional resources (e.g. stopwords.txt)
   */
  public <T> void load(NodeList nodes, String errRef, List<T> list, Class<T> clazz, String luceneMatch, ResourceLoader loader) {

    if (nodes != null) {
      for (int i = 0; i < nodes.getLength(); i++) {
        Node node = nodes.item(i);

        String name = null;
        try {
          name = DOMUtil.getAttr(node, NAME, null);
          String className = DOMUtil.getAttr(node, "class", errRef);
          final Map<String, String> params = DOMUtil.toMap(node.getAttributes());
          String configuredVersion = params.remove(LUCENE_MATCH_VERSION_PARAM);
          params.put(LUCENE_MATCH_VERSION_PARAM, luceneMatch);
          T plugin;
          if (TokenFilterFactory.class.isAssignableFrom(clazz)) {
            // this if block adds the logic that would otherwise be required for TokenFilter factories via the
            // create() method of AbstractPluginLoader...
            TokenFilterFactory factory = newInstance
                (className, TokenFilterFactory.class, getDefaultPackages(), new Class[]{Map.class}, new Object[]{params});
            factory.setExplicitLuceneMatchVersion(null != configuredVersion);

            //noinspection unchecked
            plugin = (T) factory;

          } else if (CharFilterFactory.class.isAssignableFrom(clazz)) {
            // this if block adds the logic that would otherwise be required for CharFilter factories via the
            // create() method of AbstractPluginLoader...
            CharFilterFactory factory = newInstance
                (className, CharFilterFactory.class, getDefaultPackages(), new Class[]{Map.class}, new Object[]{params});
            factory.setExplicitLuceneMatchVersion(null != configuredVersion);

            //noinspection unchecked
            plugin = (T) factory;

          } else if (TokenizerFactory.class.isAssignableFrom(clazz)) {
            // this if block adds the logic that would otherwise be required for CharFilter factories via the
            // create() method of AbstractPluginLoader...
            TokenizerFactory factory = newInstance
                (className, TokenizerFactory.class, getDefaultPackages(), new Class[]{Map.class}, new Object[]{params});
            factory.setExplicitLuceneMatchVersion(null != configuredVersion);

            //noinspection unchecked
            plugin = (T) factory;

          } else {
            // this may or may not work, but give it a shot...
            plugin = newInstance(className, clazz, NO_SUB_PACKAGES, NO_CLASSES, NO_OBJECTS);
          }
          if (plugin instanceof ResourceLoaderAware) {
            ((ResourceLoaderAware) plugin).inform(loader);
          }
          log.debug("created " + ((name != null) ? name : "") + ": " + plugin.getClass().getName());
          list.add(plugin);

        } catch (Exception ex) {
          throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Plugin init failure for " + errRef +
              (null != name ? (" \"" + name + "\"") : "") + ": " + ex.getMessage(), ex);
        }
      }
    }

  }

  /**
   * Create a new instance as per the parameters. This serves as a replacement for SolrResourceLoader.newInstance(),
   * but doesn't concern it self with SolrCoreAware, ResourceLoaderAware, or SolrInfoBean.
   *
   * @param cName        The name of the class to load
   * @param expectedType The type that should be loaded (must be passed due to type erasure)
   * @param subPackages  The package names to check
   * @param params       The types of the parameters for the constructor to be used
   * @param args         The values for the parameters to the constructor to be used
   * @param <T>          The type that will be loaded
   * @return A freshly constructed instance.
   */
  public <T> T newInstance(String cName, Class<T> expectedType, String[] subPackages, Class[] params, Object[] args) {
    Class<? extends T> clazz = findClass(cName, expectedType, subPackages);
    if (clazz == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Can not find class: " + cName);
    }

    T obj;
    try {

      Constructor<? extends T> constructor;
      try {
        constructor = clazz.getConstructor(params);
        obj = constructor.newInstance(args);
      } catch (NoSuchMethodException e) {
        //look for a zero arg constructor if the constructor args do not match
        try {
          constructor = clazz.getConstructor();
          obj = constructor.newInstance();
        } catch (NoSuchMethodException e1) {
          throw e;
        }
      }

    } catch (Error err) {
      log.error("Loading Class " + cName + " (" + clazz.getName() + ") triggered serious java error: "
          + err.getClass().getName(), err);
      throw err;

    } catch (Exception e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "Error instantiating class: '" + clazz.getName() + "'", e);
    }


    return obj;
  }

  /**
   * This method loads a class either with its FQN or a short-name (solr.class-simplename or class-simplename).
   * It tries to load the class with the name that is given first and if it fails, it tries all the known
   * solr packages. This method caches the FQN of a short-name in a static map in-order to make subsequent lookups
   * for the same class faster. The caching is done only if the class is loaded by the webapp classloader and it
   * is loaded using a short name.
   * <p>The classloader used is the current thread's class loader (instead of one from a SolrResourceLoader)</p>
   *
   * @param <T>          the generic type that the type returned should extend
   * @param cname        The name or the short name of the class.
   * @param expectedType The type which must be equal to or a super class of the returned type
   * @param subpackages  the packages to be tried if the cname starts with solr.
   * @return the loaded class. An exception is thrown if it fails
   */
  public <T> Class<? extends T> findClass(String cname, Class<T> expectedType, String... subpackages) {
    ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
    if (subpackages == null || subpackages.length == 0 || subpackages == packages) {
      subpackages = packages;
      String c = classNameCache.get(cname);
      if (c != null) {
        try {
          return Class.forName(c, true, classLoader).asSubclass(expectedType);
        } catch (ClassNotFoundException e) {
          //this is unlikely
          log.error("Unable to load cached class-name :  " + c + " for short name : " + cname + e);
        }

      }
    }

    Class<? extends T> clazz = null;
    try {
      // first try legacy analysis patterns, now replaced by Lucene's Analysis package:
      final Matcher m = legacyAnalysisPattern.matcher(cname);
      if (m.matches()) {
        final String name = m.group(4);
        log.trace("Trying to load class from analysis SPI using name='{}'", name);
        try {
          if (CharFilterFactory.class.isAssignableFrom(expectedType)) {
            return clazz = CharFilterFactory.lookupClass(name).asSubclass(expectedType);
          } else if (TokenizerFactory.class.isAssignableFrom(expectedType)) {
            return clazz = TokenizerFactory.lookupClass(name).asSubclass(expectedType);
          } else if (TokenFilterFactory.class.isAssignableFrom(expectedType)) {
            return clazz = TokenFilterFactory.lookupClass(name).asSubclass(expectedType);
          } else {
            log.warn("'{}' looks like an analysis factory, but caller requested different class type: {}", cname, expectedType.getName());
          }
        } catch (IllegalArgumentException ex) {
          // ok, we fall back to legacy loading
        }
      }

      // first try cname == full name
      try {
        return clazz = Class.forName(cname, true, classLoader).asSubclass(expectedType);
      } catch (ClassNotFoundException e) {
        String newName = cname;
        if (newName.startsWith(project)) {
          newName = cname.substring(project.length() + 1);
        }
        for (String subpackage : subpackages) {
          try {
            String name = base + '.' + subpackage + newName;
            log.trace("Trying class name " + name);
            return clazz = Class.forName(name, true, classLoader).asSubclass(expectedType);
          } catch (ClassNotFoundException e1) {
            // ignore... assume first exception is best.
          }
        }

        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "Error loading class '" + cname + "'", e);
      }

    } finally {
      if (clazz != null) {
        //cache the short name vs FQN if it is loaded by the webapp classloader  and it is loaded
        // using a short name
        if (clazz.getClassLoader() == SolrResourceLoader.class.getClassLoader() &&
            !cname.equals(clazz.getName()) &&
            (subpackages.length == 0 || subpackages == packages)) {
          //store in the cache
          classNameCache.put(cname, clazz.getName());
        }

        // print warning if class is deprecated
        if (clazz.isAnnotationPresent(Deprecated.class)) {
          log.warn("Solr loaded a deprecated plugin/analysis class [{}]. Please consult documentation how to replace it accordingly.",
              cname);
        }
      }
    }
  }

  /**
   * Converts a sequence of path steps into a rooted path, by inserting slashes in front of each step.
   *
   * @param steps The steps to join with slashes to form a path
   * @return a rooted path: a leading slash followed by the given steps joined with slashes
   */
  private String stepsToPath(String... steps) {
    StringBuilder builder = new StringBuilder();
    for (String step : steps) {
      builder.append(SLASH).append(step);
    }
    return builder.toString();
  }

  protected String getFieldTypeXPathExpressions() {
    //               /schema/fieldtype | /schema/fieldType | /schema/types/fieldtype | /schema/types/fieldType
    return stepsToPath(SCHEMA, FIELD_TYPE.toLowerCase(Locale.ROOT)) // backcompat(?)
        + XPATH_OR + stepsToPath(SCHEMA, FIELD_TYPE)
        + XPATH_OR + stepsToPath(SCHEMA, TYPES, FIELD_TYPE.toLowerCase(Locale.ROOT))
        + XPATH_OR + stepsToPath(SCHEMA, TYPES, FIELD_TYPE);
  }

  private Analyzer constructMultiTermAnalyzer(Analyzer queryAnalyzer) {
    if (queryAnalyzer == null) return null;

    if (!(queryAnalyzer instanceof TokenizerChain)) {
      return new KeywordAnalyzer();
    }
    return ((TokenizerChain) queryAnalyzer).getMultiTermAnalyzer();

  }


  protected String[] getDefaultPackages() {
    return new String[]{};
  }

}
