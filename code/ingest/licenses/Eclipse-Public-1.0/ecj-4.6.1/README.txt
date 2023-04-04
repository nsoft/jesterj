Availability of Source Code
---------------------------

The offical pom.xml for ecj indicates that original sourcce code for ecj-4.4.2 can be found at

<scm>
    <url>http://dev.eclipse.org/viewcvs/index.cgi/org.eclipse.jdt.core/</url>
    <connection>:pserver:anonymous@dev.eclipse.org:/cvsroot/eclipse</connection>
</scm>

(taken from the published pom... this has since been migrated to git with
no clear direct mapping. I'll leave it to the eclipse foundation to
explain how this satisfies ** their own ** license!!)

Another clue might be to disect whatever it is this ant build is doing:
https://github.com/eclipse-jdt/eclipse.jdt.core/blob/R4_6_1/org.eclipse.jdt.core/scripts/export-ecj.xml

Another possible option is to understand this build

https://github.com/eclipse-jdt/eclipse.jdt.core/blob/R4_6_maintenance/org.eclipse.jdt.core/pom.xml#L143

If the eclipse foundation gets their act together and makes it possible to *find* their source code I'll
be happy to tell folks where it is.