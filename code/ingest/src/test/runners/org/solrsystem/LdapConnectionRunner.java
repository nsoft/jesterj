package org.solrsystem;/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/13/13
 * Time: 4:05 PM
 */

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

  @Test
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
