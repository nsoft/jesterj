package org.solrsystem.ingest.shiro;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 10/13/13
 */

import org.apache.shiro.authc.AuthenticationInfo;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.SimpleAuthenticationInfo;
import org.apache.shiro.realm.ldap.JndiLdapRealm;
import org.apache.shiro.subject.MutablePrincipalCollection;
import org.solrsystem.ingest.model.User;
import org.solrsystem.ingest.model.impl.UserFromLdap;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import java.text.MessageFormat;
import java.util.Iterator;

public class JndiLdapRealmWithUser extends JndiLdapRealm {

  @Override
  protected AuthenticationInfo createAuthenticationInfo(AuthenticationToken token, Object ldapPrincipal, Object ldapCredentials, LdapContext ldapContext) throws NamingException {
    SimpleAuthenticationInfo authenticationInfo = (SimpleAuthenticationInfo) super.createAuthenticationInfo(token, ldapPrincipal, ldapCredentials, ldapContext);
    MutablePrincipalCollection mpc = (MutablePrincipalCollection) authenticationInfo.getPrincipals();
    final SearchControls constraints = new SearchControls();
    constraints.setSearchScope(SearchControls.SUBTREE_SCOPE);

    // get all attributes
    constraints.setReturningAttributes(null);
    String templ = getUserDnTemplate();
    String userDn = MessageFormat.format(templ, mpc.getPrimaryPrincipal());
    final NamingEnumeration<SearchResult> answer = ldapContext.search(userDn, "(objectClass=*)", constraints);

    if (answer.hasMore()) {
      Attributes attrs = answer.next().getAttributes();
      if (answer.hasMore()) {
        throw new NamingException("Non-unique user specified by:" + userDn);
      }
      //TODO: make this Guicy
      User user = new UserFromLdap(attrs, mpc);

      // at present there should only be one realm involved.
      Iterator<String> realmIter = mpc.getRealmNames().iterator();
      String firstRealm = realmIter.next();
      if (realmIter.hasNext()) {
        // ugh, need a new solution here
        String explanation = String.format("More than one realm found! (%s and %s)", firstRealm, realmIter.next());
        throw new NamingException(explanation);
      }
      mpc.add(user,firstRealm);
    } else {
      throw new NamingException("Invalid User specified by:" + userDn);
    }

    return authenticationInfo;
  }
}
