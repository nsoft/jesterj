package org.jesterj.licensereport;


import com.github.jk1.license.*;
import com.github.jk1.license.filter.DependencyFilter;

import java.util.*;
import java.util.stream.Collectors;

public class PreferredLicensesFilter implements DependencyFilter {

  ////// UNCOMMENT to VERIFY LICENSES


  private final List<String> preferenceOrder;

  public PreferredLicensesFilter(List<String> preferenceOrder) {
    this.preferenceOrder = preferenceOrder;
  }

  public ProjectData filter(ProjectData source) {
    try {
      LicenseReportExtension config = (LicenseReportExtension) source.getProject().getExtensions().getByName("licenseReport");
      List<String> configurations = Arrays.asList(config.configurations);
      //noinspection MismatchedQueryAndUpdateOfCollection
      List<String> observedLicenses = new ArrayList<>();


      List<ModuleData> modules = source.getConfigurations().stream()
          .filter(it -> configurations.contains(it.getName()))
          .flatMap(it -> it.getDependencies().stream()).collect(Collectors.toList());

      for (ModuleData module : modules) {
        final License[] preferred = {null};

        module.getLicenseFiles().stream().flatMap(it -> it.getFileDetails().stream()).forEach(details -> {
          String license = details.getLicense();
          // NOTICE file is not a valid source of licenses for the project itself!!
          // https://github.com/jk1/Gradle-License-Report/issues/335
          if (license != null && (details.getFile() == null || !details.getFile().contains("NOTICE"))) {
            observedLicenses.add(license);
            if (preferenceOrder.contains(license) && (preferred[0] == null ||
                (preferenceOrder.indexOf(license) < preferenceOrder.indexOf(preferred[0].getName())))) {
              System.out.println("Preferring " + license + " from license file "+details.getFile()+" over " + preferred[0] + " for " + module.getName());
              preferred[0] = new License(license, details.getLicenseUrl());
            } else {
              System.out.println("Ignoring " + license+ " from license file "+details.getFile()+ " because " + (preferred[0] != null ? preferred[0].getName() : null) + " is better for " + module.getName());
            }
          }
        });

        // Manifest is more distant than license file so only consult it if we don't have an answer yet
        // see https://issues.apache.org/jira/browse/LEGAL-640
        if (preferred[0] == null) {
          for (ManifestData manifest : module.getManifests()) {
            String license = manifest.getLicense();
            if (license != null) {
              observedLicenses.add(license);
              if (preferenceOrder.contains(license) && (preferred[0] == null ||
                  (preferenceOrder.indexOf(license) < preferenceOrder.indexOf(preferred[0].getName())))) {
                System.out.println("Preferring " + license + " from manifest over " + (preferred[0] != null ? preferred[0].getName() : null) + " for " + module.getName());
                preferred[0] = new License(license, manifest.getLicenseUrl());
              } else {
                System.out.println("Ignoring " + license + " from manifest file because " + (preferred[0] != null ? preferred[0].getName() : null) + " is better for " + module.getName());
              }
            }
          }
        }

        // We consider pom more distant than license file or manifest so only consult it if we don't have an answer yet
        // see https://issues.apache.org/jira/browse/LEGAL-640
        if (preferred[0] == null) {
          for (PomData pom : module.getPoms()) {
            Set<License> licenses = pom.getLicenses();
            if (licenses != null) {
              observedLicenses.addAll(licenses.stream().map(License::getName).collect(Collectors.toList()));
              for (License license : licenses) {
                if (preferenceOrder.contains(license.getName()) && (preferred[0] == null ||
                    (preferenceOrder.indexOf(license.getName()) < preferenceOrder.indexOf(preferred[0].getName())))) {
                  System.out.println("Preferring " + license.getName() + " from pom over " + (preferred[0] != null ? preferred[0].getName() : null) + " for " + module.getName());
                  preferred[0] = license;
                } else {
                  System.out.println("Ignoring " + license.getName() + " from pom because " + (preferred[0] != null ? preferred[0].getName() : null) + " is better for " + module.getName());
                }
              }
            }
          }
        }

        // Having sorted through the licenses and found our favorite, we now need to ensure that that license
        // is the only one represented for the dependency (by overwriting/replacing what we were originally given)

        if (preferred[0] != null) {
          preferred[0].setName(preferred[0].getName());
          PomData pom = module.getPoms().stream().findFirst().orElse(null);
          if (pom != null) {
            //System.out.println("Preferring " + preferred[0] + " for " + pom.getName() + "(" + observedLicenses + ")");
            pom.setLicenses(new TreeSet<>(Set.of(preferred[0])));
            pom.setName("Artificial Pom for " + module.getName());
            module.setPoms(Set.of(pom));
            module.setManifests(Set.of());
            module.setLicenseFiles(Set.of());
          } else if (module.getManifests() != null && !module.getManifests().isEmpty()) {
            ManifestData manifestData = module.getManifests().iterator().next();
            manifestData.setLicense(preferred[0].getName());
            module.setManifests(new TreeSet<>(Set.of(manifestData)));
            module.setLicenseFiles(Set.of());
          } else if (module.getLicenseFiles() != null && !module.getLicenseFiles().isEmpty()) {
            LicenseFileData licenseFileData = module.getLicenseFiles().iterator().next();
            LicenseFileDetails details = licenseFileData.getFileDetails().iterator().next();
            details.setFile("");
            details.setLicenseUrl("");
            licenseFileData.setFileDetails(new TreeSet<>(Set.of(details)));
            module.setLicenseFiles(Set.of(licenseFileData));
          } else {
            System.out.println("No place to store preferred license??? " + module );
          }
        } else {
          System.out.println("No preferred license for " + module.getName() + "(" + observedLicenses + ")");
        }

      }
    } catch (Throwable t) {
      System.out.println("Caught " + t);
      StackTraceElement[] stackTrace = t.getStackTrace();
      for (StackTraceElement stackTraceElement : stackTrace) {
        System.out.println(stackTraceElement);
      }
      throw  t;
    }

    return source;
  }


}