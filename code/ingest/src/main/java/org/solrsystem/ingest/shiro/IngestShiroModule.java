package org.solrsystem.ingest.shiro;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/3/13
 * Time: 12:17 AM
 */


import com.google.inject.Provides;
import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.authc.credential.Sha512CredentialsMatcher;
import org.apache.shiro.config.Ini;
import org.apache.shiro.guice.web.ShiroWebModule;
import org.apache.shiro.realm.text.IniRealm;

import javax.servlet.ServletContext;

public class IngestShiroModule extends ShiroWebModule {

  public IngestShiroModule(ServletContext servletContext) {
    super(servletContext);
  }

  protected void configureShiroWeb() {
    try {
      bindRealm().toConstructor(IniRealm.class.getConstructor(Ini.class));
    } catch (NoSuchMethodException e) {
      addError(e);
    }
    HashedCredentialsMatcher foo;
  }

  @Provides
  Ini loadShiroIni() {
    return Ini.fromResourcePath("classpath:shiro.ini");
  }
}