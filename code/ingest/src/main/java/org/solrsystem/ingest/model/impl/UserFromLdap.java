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
