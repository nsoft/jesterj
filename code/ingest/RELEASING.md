# Key tasks prior to release

## Prerequisites

1. Key for signing the release
2. Login credentials for sonatype's nexus

## Licenses

The management of licenses is one of the biggest challenges in this project. There are over 300 possible licenses available for something like 250 dependencies, and we must declare which license we choose and then explicitly comply with the requirements of the license we chose.

We attempt to follow https://infra.apache.org/licensing-howto.html#example-notice and https://www.apache.org/legal/resolved.html so in any case where you notice we might not be adhering to that standard, stop the release and fix it or initiate a discussion at https://github.com/nsoft/jesterj/discussions/categories/general

1. Run `./gradlew clean check` - this will check that
   1. Gradle License Report can find an acceptable license for every transitive dependency
      included in the node jar distributable. If you get messages about not being able to
      find a valid license, [dependencies-without-allowed-license.json](build/reports/dependency-license/dependencies-without-allowed-license.json) will identify the offenders. To resolve this, search the build output for the library module name, looking for output such as
      ```
      Ignoring LICENSE.txt from manifest file because null is better for jai-imageio-core
      Preferring BSD-3-Clause-Nuclear from pom over null for jai-imageio-core
      Preferring Apache-2.0 from pom over null for jamm
      Preferring Apache-2.0 from manifest over null for jffi
      Preferring Apache-2.0 from pom over null for jnr-a64asm
      Preferring Apache-2.0 from manifest over null for jnr-constants
      Preferring Apache-2.0 from manifest over null for jnr-ffi
      Preferring Eclipse-Public-2.0 from pom over null for jnr-posix
      Ignoring GNU General Public License Version 2 from pom because Eclipse-Public-2.0 is better for jnr-posix
      Ignoring GNU Lesser General Public License Version 2.1 from pom because Eclipse-Public-2.0 is better for jnr-posix
      ```
      This will tell you what options were considered. Evaluation order is controlled by
      the order of the list passed to PreferredLicensesFilter in [build.gradle](./build.gradle#L174). If a valid license was discarded
      then there is a bug in that class. If not, then we must find a way to explicitly
      document an acceptable license or remove it from the library. Acceptable licenses are
      defined in [allowed-licenses.json](licenses/allowed-licenses.json) licenses are
      identified by the patterns in
      [license-normalizer-bundle.json](licenses/license-normalizer-bundle.json)
   2. Next the build will check that we have proper documentation from which to assemble a
      notice file (this is also included wholesale for avoidance of doubt). If documentation
      is missing you will see messages such as:
      ````
      FAIL: library level README.txt not found at /home/gus/projects/jesterj/code/jesterj/code/ingest/licenses/Apache-2.0/hppc-0.10.0/README.txt
      FAIL: library level NOTICE.txt not found at /home/gus/projects/jesterj/code/jesterj/code/ingest/licenses/Apache-2.0/hppc-0.10.0/NOTICE.txt
      FAIL: library level README.txt not found at /home/gus/projects/jesterj/code/jesterj/code/ingest/licenses/Apache-2.0/jackson-annotations-2.18.0/README.txt
      FAIL: library level NOTICE.txt not found at /home/gus/projects/jesterj/code/jesterj/code/ingest/licenses/Apache-2.0/jackson-annotations-2.18.0/NOTICE.txt
      FAIL: library level README.txt not found at /home/gus/projects/jesterj/code/jesterj/code/ingest/licenses/Apache-2.0/jackson-core-2.18.0/README.txt
      FAIL: library level NOTICE.txt not found at /home/gus/projects/jesterj/code/jesterj/code/ingest/licenses/Apache-2.0/jackson-core-2.18.0/NOTICE.txt
      ````
      This is typically corrected for EACH library by
      1. Navigating to the directory for the license type
         ([licenses/Apache-2.0](licenses/Apache-2.0) for the examples above).
      2. Checking for a differently versioned module directory. If one is found
         1. Open the README.txt inside, where you should find a URL identifying the
            location we found the prior information. Usually this is github or other
            online VCS
         1. Once the proper location for the license information for the current version
            is located,
            1. For an Apache 2.0 licensed library that has a notice file, verify that the contents of that notice file have not changed.
            2. For an Apache 2.0 licensed library where we have not previously found a
               notice file, follow the verification link in the README.txt, verify that
               the link is looking at a tag, or branch that matches the version we are
               licensing, and then verify that there is (still) no notice file available at
               the link destination, or at the top level of the associated repository. In a
               few cases there may be no repository link (possibly because the only source
               available was the artifact itself), so use your judgement in those cases.
            3. For any Libraries that have a requirement to reproduce the copyright notice
               (license types of MIT, BSD, etc.), follow the verification link and ensure
               that the copyright attribution is still the same
            4. For any of the less common licenses read the license level readme carefully
               and verify the proper compliance.
         1. Rename the module directory with the correct version name and re-run the build.
            If you got it mostly right the `FAIL:` message should disappear
      1. If there is no prior directory then this is a brand-new dependency, and you need
         to create a new module directory with the correct name. Work by example, and
         ask questions in GitHub Discussions if there is no clear example.
   1. Next the build checks for obsolete license documentation directories, delete any
      it finds, and re-run.
   2. Next the build checks for links in license documentation that do not return
      200 OK and reports them. Find alternate links for any that have become broken
      or fix any typos
   2. Next the build verifies that the Code compiles
   1. Then it runs the unit tests, and some other misc checks.
   1. If all is well you should see `BUILD SUCCESS` if not, troubleshoot.
1. Next, Run `./gradlew verifyLicenseDirs` which will extract every URL from every
   README.txt and ensure that it is not a dead link. Fix any errors



Things to improve in the future (PR's welcome!):
1. Automate notice file updates (generate based on template parsed out of README.txt?)
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
