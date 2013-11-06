/*
 * Copyright 2013 Needham Software LLC
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

package org.solrsystem.ingest.vaadin.views;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 11/4/13
 */

import com.vaadin.server.UserError;
import com.vaadin.ui.*;
import org.apache.shiro.SecurityUtils;
import org.apache.shiro.authc.*;
import org.apache.shiro.subject.Subject;
import org.solrsystem.ingest.model.User;

import javax.inject.Inject;

public class LogIn extends CustomComponent {

  private GridLayout loginLayout = new GridLayout(3, 3);
  private FormLayout loginForm = new FormLayout();
  private Label loginLabel = new Label("Please Log in.");
  private TextField userNameTextField = new TextField("User");
  private PasswordField passwordField = new PasswordField("Password");
  private Button loginSubmit = new Button("Log In");
  private UsernamePasswordToken authToken;

  public LogIn() {

  loginLayout.setWidth("100%");
  loginLayout.setHeight("300px");
  loginForm.setSpacing(true);
  loginLabel.setImmediate(true);
  userNameTextField.setRequired(true);
  passwordField.setRequired(true);
  loginSubmit.addClickListener(new Button.ClickListener() {
    @Override
    public void buttonClick(Button.ClickEvent event) {
      authToken.setUsername(userNameTextField.getValue());
      authToken.setPassword(passwordField.getValue().toCharArray());
      try {
        Subject currentUser = SecurityUtils.getSubject();
        currentUser.login(authToken);
        if (currentUser.isAuthenticated()) {
          User user = currentUser.getPrincipals().oneByType(User.class);
          loginLabel.setValue("Hello " + user.getDisplayName());
        }
      } catch (UnknownAccountException uae) {
        userNameTextField.setComponentError(new UserError("Unknown User"));
        loginLabel.setValue("That user does not exist");
      } catch (IncorrectCredentialsException ice) {
        loginLabel.setValue("Invalid password");
        passwordField.setComponentError(new UserError("Invalid Password"));
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
  loginForm.addComponent(loginLabel);
  loginForm.addComponent(userNameTextField);
  loginForm.addComponent(passwordField);
  loginForm.addComponent(loginSubmit);
  loginLayout.addComponent(loginForm, 1, 1);
  }

  @Inject
  public void setToken(UsernamePasswordToken token) {
    this.authToken = token;
  }

}
