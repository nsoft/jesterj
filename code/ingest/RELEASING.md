# Key tasks prior to release

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

