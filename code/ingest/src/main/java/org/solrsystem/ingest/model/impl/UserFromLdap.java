package org.solrsystem.ingest.model.impl;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/14/13
 */

import org.apache.shiro.subject.PrincipalCollection;
import org.solrsystem.ingest.model.User;

import javax.naming.NamingException;
import javax.naming.directory.Attributes;

public class UserFromLdap implements User {

  String userId;
  String displayName;

  public UserFromLdap(Attributes attrs, PrincipalCollection pc) throws NamingException {
    this.userId = String.valueOf(pc.getPrimaryPrincipal());
    this.displayName = String.valueOf(attrs.get("displayName").get());
  }

  public String getUserId() {
    return userId;
  }

  public String getDisplayName() {
    return displayName;
  }
}
