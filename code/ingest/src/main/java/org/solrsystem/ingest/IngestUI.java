package org.solrsystem.ingest;

import com.vaadin.server.VaadinRequest;
import com.vaadin.ui.*;
import org.apache.shiro.authc.UsernamePasswordToken;

import javax.inject.Inject;

public class IngestUI extends UI {

  private  UsernamePasswordToken authToken;

  @Override
  protected void init(VaadinRequest request) {
    VerticalLayout vl = new VerticalLayout();
    Label loginLabel = new Label("Please Log in.");
    final TextField userName = new TextField("User");
    final TextField password = new TextField("Password");
    Button submit = new Button("Log In");
    submit.addClickListener(new Button.ClickListener() {

      @Override
      public void buttonClick(Button.ClickEvent event) {
        authToken.setUsername(userName.getValue());
        authToken.setPassword(password.getValue().toCharArray());
      }
    });
    vl.addComponent(loginLabel);
    vl.addComponent(userName);
    vl.addComponent(password);
    vl.addComponent(submit);
    setContent(vl);

  }


  @Inject
  public void setToken(UsernamePasswordToken token) {
    this.authToken = token;
  }
}