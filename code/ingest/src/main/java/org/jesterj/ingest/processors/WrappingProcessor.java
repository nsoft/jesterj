package org.jesterj.ingest.processors;

import com.copyright.easiertest.SimpleProperty;
import org.jesterj.ingest.model.Document;
import org.jesterj.ingest.model.DocumentProcessor;
import org.jesterj.ingest.model.impl.NamedBuilder;
import org.jetbrains.annotations.NotNull;

/**
 * A processor that can execute arbitrary code before/after an existing processor. Note that
 * it is an anti-pattern to build a chain of WrappingProcessors one wrapping the other because
 * this defeats the fault tolerance features of the framework by turning several steps into
 * a single step.
 */
@SuppressWarnings("unused")
public class WrappingProcessor implements DocumentProcessor {
  private String name;
  private DocumentProcessor wrapped;

  @Override
  public Document[] processDocument(Document document) {
    Document[] documents = null;
    try {
      before(document);
      documents = getWrapped().processDocument(document);
      return success(documents);
    } catch (Exception e) {
      Document[][] results = wrapRef(documents);
      if (error(e, document, results)) {
        throw e;
      } else {
        // the call to error() should have mutated results to comply with these contracts before returning false
        if (results[0] == null) {
          throw new RuntimeException("Invalid WrappingProcessor implementation. Contract violation: " +
              "results[0] is null");
        }
        Document[] result = results[0];
        for (int i = 0; i < result.length; i++) {
          Document document1 = result[i];
          if (document1 == null) {
            throw new RuntimeException("invalid WrappingProcessor implementation Contract violation: " +
                "results[0] contains a null document at position " + i);
          }
        }
        return result;
      }
    } finally {
      always(document);
    }
  }

  @NotNull
  Document[][] wrapRef(Document[] documents) {
    return new Document[][]{documents};
  }

  @Override
  @SimpleProperty
  public String getName() {
    return name;
  }

  /**
   * Execute some code before a document is processed. Possibly used to cache field values or
   * clear threadlocal variables etc. If this method throws an exception the wrapped
   * processor will not execute, but error and always will.
   *
   * @param document the document to be processed by the wrapped processor
   */
  public void before(Document document) {
    // do something before the document is processed
  }

  /**
   * Override to execute some code after a document has successfully been processed by the
   * wrapped processor. Successful is defined as "not throwing an exception". If this method
   * throws an exception it will be processed by the error method of this class, but the
   *
   * @param results the document(s) produced by the action of the wrapped procesor
   * @return the (possibly edited) documents to be passed on to subsequent steps. No element
   * of this array should be null. If documents need to be removed, then a new
   * array should be created omitting the desired documents.
   */
  public Document[] success(Document[] results) {
    // do something after the document is successfully process
    return results;
  }

  /**
   * Override to execute some code in the even that the wrapped processor throws an exception.
   * The exception can be swallowed by returning false, but the default implementation
   * returns true. It is possible to use this method to entirely edit the reslts of processing
   * and continue (or not) in the event of an error. The return value controls whether or not
   * the error is re-thrown, or the code may simply throw it's own preferred RuntimeException.
   * If the error is not re-thrown this method must verify and ensure that results[0] contains
   * an array of at least one document and no null documents.
   *
   * @param e        the exception that lead to the invocation of this method.
   * @param original the document that caused the exception
   * @param results  1 by N array of documents setting a new array into results[0] will
   *                 replace the results entirely, whereas setting results[0][n] will replace
   *                 a single document. If this method returns false (indicating no error)
   *                 then results[0] will be passed to the next step.
   * @return true if the exception should be re-thrown, or false if it should be swallowed.
   */
  public boolean error(Exception e, Document original, Document[][] results) {
    // do something after the prior processing throws an exception
    return true;
  }

  /**
   * Override to execute some code regardless of the success, failure or any other
   * machinations of any of the other methods. This can be used for things like
   * clearing thread locals or other code that really must run no matter how ugly
   * the prior processing gets. This is the sole method executed in a finally block
   * following the processing of all other methods. The following implementation is
   * guaranteed:
   * <pre>
   *
   *   public Document[] processDocument(Document document) {
   *     // nothing that can throw/fail here
   *     try {
   *       // stuff that might succceed
   *     } catch (Exception e) {
   *       // anything we do if things go wrong
   *     } finally {
   *       always(document);
   *     }
   *   }
   * </pre>
   *
   * @param original the original document that was passed to the processDocumentMethod.
   *                 NOTE: this is not a copy of the document as it was, but the result
   *                 whatever processing was attempted. If you need a faithful copy of the
   *                 original you should make one in an overridden before() method, hold
   *                 it in a field and use/clear the field here.
   */
  public void always(Document original) {
    // something that must happen regardless of the result of the processing
  }

  public void setName(String name) {
    this.name = name;
  }

  @SimpleProperty
  public DocumentProcessor getWrapped() {
    return wrapped;
  }

  public void setWrapped(DocumentProcessor wrapped) {
    this.wrapped = wrapped;
  }


  public static class Builder extends NamedBuilder<WrappingProcessor> {

    WrappingProcessor obj = new WrappingProcessor();

    @Override
    public WrappingProcessor.Builder named(String name) {
      getObj().name = name;
      return this;
    }

    public WrappingProcessor.Builder wrapping(DocumentProcessor wrapped) {
      getObj().setWrapped(wrapped);
      return this;
    }

    private void setObj(WrappingProcessor obj) {
      this.obj = obj;
    }

    public WrappingProcessor build() {
      WrappingProcessor object = getObj();
      setObj(new WrappingProcessor());
      return object;
    }

  }

}
