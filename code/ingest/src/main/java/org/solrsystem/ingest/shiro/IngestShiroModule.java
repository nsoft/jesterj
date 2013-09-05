package org.solrsystem.ingest.shiro;

/*
 * Created with IntelliJ IDEA.
 * User: gus
 * Date: 9/3/13
 * Time: 12:17 AM
 */


import com.google.inject.Provides;
import org.apache.shiro.config.Ini;
import org.apache.shiro.guice.ShiroModule;
import org.apache.shiro.realm.text.IniRealm;

public class IngestShiroModule extends ShiroModule {
  protected void configureShiro() {
    try {
      bindRealm().toConstructor(IniRealm.class.getConstructor(Ini.class));
    } catch (NoSuchMethodException e) {
      addError(e);
    }
  }

  @Provides
  Ini loadShiroIni() {
    return Ini.fromResourcePath("classpath:shiro.ini");
  }
}