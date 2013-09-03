package org.solrsystem.ingest.guice;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/2/13
 * Time: 9:49 PM
 */

import com.vaadin.server.UIClassSelectionEvent;
import com.vaadin.server.UICreateEvent;
import com.vaadin.server.UIProvider;
import com.vaadin.ui.UI;

import javax.inject.Inject;

public class IngestUiProvider extends UIProvider {
  @Inject
  private Class<? extends UI> uiClass;

  @Override
  public UI createInstance(UICreateEvent event) {
    return IngestServletConfig.injector().getProvider(uiClass).get();
  }

  @Override
  public Class<? extends UI> getUIClass(UIClassSelectionEvent event) {
    return uiClass;
  }

}
