package org.jesterj.ingest.processors;

import java.net.http.HttpResponse;

public class BatchFailureExceptionOpenSearch extends Exception {
  HttpResponse<String> response;
  public BatchFailureExceptionOpenSearch(String s, HttpResponse<String> resp) {
    super(s);
    response = resp;
  }
}
