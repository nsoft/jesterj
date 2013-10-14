package org.solrsystem.ingest;

import com.vaadin.server.UserError;
import com.vaadin.server.VaadinRequest;
import com.vaadin.ui.*;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.*;
import org.apache.shiro.subject.Subject;

import javax.inject.Inject;

public class IngestUI extends UI {

  private UsernamePasswordToken authToken;

  @Override
  protected void init(VaadinRequest request) {
    GridLayout gridLayout = new GridLayout(3, 3);
    gridLayout.setWidth("100%");
    gridLayout.setHeight("300px");
    FormLayout formLayout = new FormLayout();
    formLayout.setSpacing(true);
    final Label loginLabel = new Label("Please Log in.");
    loginLabel.setImmediate(true);
    final TextField userName = new TextField("User");
    userName.setRequired(true);
    final PasswordField password = new PasswordField("Password");
    password.setRequired(true);
    final Button submit = new Button("Log In");
    submit.addClickListener(new Button.ClickListener() {

      @Override
      public void buttonClick(Button.ClickEvent event) {
        authToken.setUsername(userName.getValue());
        authToken.setPassword(password.getValue().toCharArray());
        try {
          Subject currentUser = SecurityUtils.getSubject();
          currentUser.login(authToken);
          if (currentUser.isAuthenticated()) {
            loginLabel.setValue("Hello " + currentUser.getPrincipal().toString());
          }
        } catch (UnknownAccountException uae) {
          userName.setComponentError(new UserError("Unknown User"));
          loginLabel.setValue("That user does not exist");
        } catch (IncorrectCredentialsException ice) {
          loginLabel.setValue("Invalid password");
          password.setComponentError(new UserError("Invalid Password"));
        } catch (LockedAccountException lae) {
          loginLabel.setValue("Account is locked. Contact your System Administrator");
          loginLabel.setComponentError(new UserError("Account locked"));
        } catch (ExcessiveAttemptsException eae) {
          loginLabel.setValue("Too many login failures.");
          loginLabel.setComponentError(new UserError("Failures Exceeded"));
        } catch (Exception e) {
          e.printStackTrace();
          loginLabel.setValue("Internal Error:" + e.getMessage());
        }
      }

    });
    formLayout.addComponent(loginLabel);
    formLayout.addComponent(userName);
    formLayout.addComponent(password);
    formLayout.addComponent(submit);
    gridLayout.addComponent(formLayout, 1, 1);
    setContent(gridLayout);

  }


  @Inject
  public void setToken(UsernamePasswordToken token) {
    this.authToken = token;
  }
}