package org.jseterj.example.wikidocs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.Status;
import org.jesterj.ingest.model.impl.NamedBuilder;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;

/**
 * A processor that can create fields for the elements typically found in a "line file docs" file used by the
 * lucene nightly benchmarks (see <a href="https://github.com/mikemccand/luceneutil">lucene-util</a>)
 */
public class ParseWikidoc implements DocumentProcessor {
  private static final Logger log = LogManager.getLogger();

  private String name;
  final SimpleDateFormat dateParser = new SimpleDateFormat("dd-MMM-yyyy HH:mm:ss", Locale.US);

  public static final String[] FIELDS = new String[] {
      "title",
      "lastMod",
      "body",
      "randomLabel"
  };

  @Override
  public Document[] processDocument(Document document) {
    String id = document.getId();
    for (String field : new HashSet<>(document.keySet())) {
      document.removeAll(field);
    }
    document.put("id", id); // lucene util uses a long, but this woudl break JesterJ, so can't change id until send to solr
    log.trace("ParseWikiStarted:{}", id);
    String s = new String(document.getRawData());
    if (s.startsWith("FIELDS_HEADER_INDICATOR###")) {
      log.info("Dropping {}", document);
      document.setStatus(Status.DROPPED, "header row dropped");
      log.info("Dropped {}", document);
    } else {
      document.setRawData(null); // ensure we don't try to write a default search field
      String[] cols = s.split("\t");
      for (int i = 0; i < FIELDS.length; i++) {
        if (cols.length <= i) {
          break;
        }
        document.put(FIELDS[i],cols[i]);
      }
      mimickLucneUtilDate(document);
    }
    return new Document[]{document};
  }

  private void mimickLucneUtilDate(Document document) {
    String lastMod = document.getFirstValue("lastMod");
    Date parsed;
    try {
      parsed = dateParser.parse(lastMod);
    } catch (ParseException e) {
      parsed = new Date(0);
    }
    document.removeAll("lastMod");
    document.put("lastMod", String.valueOf(parsed.getTime()));
  }

  @Override
  public String getName() {
    return name;
  }

  public static class Builder extends NamedBuilder<ParseWikidoc> {

    ParseWikidoc obj = new ParseWikidoc();

    protected ParseWikidoc getObj() {
      return obj;
    }

    public Builder named(String name) {
      getObj().name = name;
      return this;
    }

    private void setObj(ParseWikidoc obj) {
      this.obj = obj;
    }

    public ParseWikidoc build() {
      ParseWikidoc object = getObj();
      setObj(new ParseWikidoc());
      return object;
    }

  }
}
