package org.jesterj.ingest.processors;

import java.net.http.HttpResponse;

public class OpenSearchBatchFailureException extends Exception {
  HttpResponse<String> response;
  public OpenSearchBatchFailureException(String s, HttpResponse<String> resp) {
    super(s);
    response = resp;
  }
}
