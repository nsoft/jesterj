# Key tasks prior to release

## Prerequisites

1. Key for signing the release
2. Login credentials for sonatype's nexus

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

## Publishing ingest to Central

Only the jar file containing our classes (and associated src/javadoc jars) is published to the central repository, since that is the artifact that should be imported by folks attempting to create an ingestion plan.

1. Change the version to end in -LOCAL (i.e. change `version = '1.0-SNAPSHOT'` to `version = '1.0-LOCAL'`)
2. Verify that there are no -LOCAL versioned artifacts in your `~/.m2/repository`
2. Publish to maven local with `./gradlew publishToMavenLocal`
3. View the artifacts that have been created with -LOCAL suffix in your `~/.m2/repository`. Particularly verify that the artifact coordinates and license are correct.
4. Edit the gradle.build file in `/examples/shakespeare` to use the newly published -LOCAL version.
5. In the shakespeare example run `./gradlew clean build`. This serves as a validation that the artifacts are well formed and support at least basic compilation. If there are compile errors there are two possible reasons. One is that the example has become stale and is out-dated, and the other is that there is an issue with the artifacts just created. Diagnose and fix one of these problems and try again.
6. Remove the -LOCAL from both the example and this module.
7. Ensure that there are no uncommitted changes, including random local debugging files like `foo.json`
8. Create a branch for the release. If this is a major release with version M.0.0, or a Beta release thereof, the new branch should be named branch_Mx (whe M is the major version number). If this is a minor release M.N.0 a branch name branch_M_N should be created. Bugfix releases M.N.X can be released directly from branch_M_N (but will be denoted with a tag later in the process)
9. Changing only the version in the build.gradle file to the intended release version, commit and push the branch. Ideally this should be the only commit before the artifacts are released, but of course if there is something wrong, and it has to be fixed you will need to commit that too. **Be sure to carefully backport any fixes to the branch_Mx or master branches as appropriate**.
10. From a computer with the appropriate username and password in ~/.gradle/gradle.properties run `./gradlew publishMavenJavaPublicationToSonatypeRepository`
11. Log into https://oss.sonatype.org/
12. After clicking on staging profiles you should see the artifacts you just uploaded. Brows through and ensure that
    1. Verify that the LICENSE.txt, NOTICE.txt and the licenses directory have been included under META-INF
    2. Using the archive browsing feature view the Manifest file for the jars, and ensure that it does NOT say `(with uncommited files)` after the commit hash.
    3. Everything else looks copacetic. There is no cost and no reason not to republish to the staging repo a dozen times or more if necessary. Nobody will get upset, but if you release a bad artifact, that's a horrible mess requiring a whole new release to fix, and it creates a pitfall for our users if they happen to grab the bad version.
4. After you are satisfied with the artifacts, click close.
5. Wait for Sonatype to run it's verification checks. If anything fails fix it and then return to the step where you publish to sonatype.
6. Take a deep breath. This is the point of no return. Anything you did before this, nobody knows about it, and it can be re-done. This next step is permanent.
6. Click Release in the sonatype nexus repository manager


## Publishing the executable
The executable "node" jar created during the deployment process should be uploaded to the github release page and advertised in the top level README file. It is highly desirable to use the jar that was generated at the time the maven jars were published. It is REQUIRED that the node jar be from the same commit hash.

## Tagging

The release should be tagged at the time the node jar is uploaded to Github. Tag names should be the simple verbatim release version number (including -betaN if it's the Nth beta release, etc.)

## Announcing
Wait 12h before announcing the release, so that artifacts have time to spread to central and all it's mirrors.
