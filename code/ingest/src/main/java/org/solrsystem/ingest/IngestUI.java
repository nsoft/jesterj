package org.solrsystem.ingest;

import com.vaadin.server.VaadinRequest;
import com.vaadin.ui.UI;
import org.solrsystem.ingest.vaadin.views.LogIn;

import javax.inject.Inject;

public class IngestUI extends UI {


  LogIn loginView;

  @Override
  protected void init(VaadinRequest request) {
    setContent(loginView);
  }

  @Inject
  public void setLoginView(LogIn logIn) {
    this.loginView = logIn;
  }

}