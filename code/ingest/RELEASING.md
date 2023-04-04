# Key tasks prior to release

## Licenses

The management of licenses is one of the biggest challenges in this project. There are over 300 possible licenses available for something like 250 dependencies, and we must declare which license we choose and then explicitly comply with the requirements of the license we chose.

We attempt to follow https://infra.apache.org/licensing-howto.html#example-notice and https://www.apache.org/legal/resolved.html so in any case where you notice we might not be adhering to that standard, stop the release and fix it or initiate a discussion at https://github.com/nsoft/jesterj/discussions/categories/general

1. Check out and build a local copy of gradle license report
1. Apply this patch: https://patch-diff.githubusercontent.com/raw/jk1/Gradle-License-Report/pull/265.patch
1. Build and deploy to maven local repo
1. Add or verify mavenLocal() in the pluginManagment section of settings.gradle
1. Adjust the version of the license report plugin in the dependencies block of code/ingest/buildSrc/build.gradle to match the version deployed to mavenLocal()
1. Run ` ./gradlew verifyLicenseDirs` and fix any errors
1. Run `./gradlew checkLicense` and fix any errors
1. For EACH library documented in `./licenses` read the README.txt, and verify any hyperlinks (TODO: Automate that), fix any links that look wrong. The most common error is a cut and paste error where a reference to the library that was copied has not been edited, so be on the lookout for that.
1. For EACH Apache 2.0 licensed library that has a notice file, verify that the contents of that notice file have not changed.
1. For each Apache 2.0 licensed library where we have not previously found a notice file, follow the verification link in the README.txt, verify that the link is looking at a tag, or branch that matches the version we are licensing, and then verify that there is no notice file available at the link destination, or at the top level of the associated repository. In a few cases there may be no repository link (possibly because the only source available was the artifact itself), so use your judgement in those cases.
2. For any Libraries that have a requirement to reproduce the copyright notice (license types of MIT, BSD, etc), follow the verification link and ensure that the copyright attribution is still the same
3. For any of the less common licenses read the license level readme carefully and verify the proper compliance.

Things to improve in the future (PR's welcome!):
1. Automate verification of links in read readme files not going dead
2. Automate notice file updates (generate based on template parsed out of README.txt?)
3. Automate scraping of Copyrights

