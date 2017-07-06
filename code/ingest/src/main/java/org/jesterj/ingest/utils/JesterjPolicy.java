package org.jesterj.ingest.utils;

import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;

/**
 * Custom security policy for JesterJ. This is presently permissive, but in the future we will
 * likely try to restrict permissions for code loaded by jini.
 */
public class JesterjPolicy extends Policy {
  final Permissions pc = new Permissions();

  public JesterjPolicy() {
    pc.add(new AllPermission());
  }

  @Override
  public PermissionCollection getPermissions(CodeSource codesource) {
    return pc;
  }

  @Override
  public PermissionCollection getPermissions(ProtectionDomain domain) {
    return pc;
  }

  @Override
  public boolean implies(ProtectionDomain domain, Permission permission) {
    return true;
  }

  @Override
  public String toString() {
    return "JesterJ all perms policy";
  }
}
