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

package org.solrsystem;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/13/13
 * Time: 4:05 PM
 */

import org.junit.Ignore;
import org.junit.Test;

import javax.naming.Context;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

public class LdapConnectionRunner {

  @Test
  public void testSimpleAuth() throws Exception {
    Hashtable env = new Hashtable();
    env.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");

    env.put(Context.SECURITY_AUTHENTICATION, "simple");
    env.put(Context.SECURITY_PRINCIPAL, "cn=denta, o=solrsystem, dc=needhamsoftware,dc=com");
    env.put(Context.SECURITY_CREDENTIALS, "mostlyharmless");
    DirContext ctx = new InitialDirContext(env);
  }

  @Test @Ignore // Not working, I suspect due to issues on the LDAP SERVER...
  public void testDigestMD5Auth() throws Exception {
    Hashtable env = new Hashtable();
    env.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.sun.jndi.ldap.LdapCtxFactory");
    env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");

    env.put(Context.SECURITY_AUTHENTICATION, "DIGEST-MD5");
    env.put(Context.SECURITY_PRINCIPAL, "cn=denta, o=solrsystem, dc=needhamsoftware,dc=com");
    env.put(Context.SECURITY_CREDENTIALS, "mostlyharmless");
    DirContext ctx = new InitialDirContext(env);

  }


//  public static void main(String[] args) throws NamingException {
//    // Set up the environment for creating the initial context
//    Hashtable env = new Hashtable();
//    env.put(Context.INITIAL_CONTEXT_FACTORY,
//        "com.sun.jndi.ldap.LdapCtxFactory");
//    env.put(Context.PROVIDER_URL, "ldap://localhost:10389/");
//
//// Authenticate as S. User and password "mysecret"
////    env.put(Context.SECURITY_AUTHENTICATION, "DIGEST-MD5");
//    env.put(Context.SECURITY_AUTHENTICATION, "simple");
//    env.put(Context.SECURITY_PRINCIPAL, "cn=denta, o=solrsystem, dc=needhamsoftware,dc=com");
//    env.put(Context.SECURITY_CREDENTIALS, "mostlyharmless");
////    env.put("java.naming.security.sasl.realm", "aafdadf");
//// Create the initial context
//    DirContext ctx = new InitialDirContext(env);
//
//// ... do something useful with ctx
//  }
}
