Copyright Declaration
---------------------

http://www.adobe.com/devnet/xmp/library/eula-xmp-library-java.html

The above notice was found here:

http://www.adobe.com/devnet/xmp/library/eula-xmp-library-java.html

The above site was linked from

http://grepcode.com/snapshot/repo1.maven.org/maven2/com.adobe.xmp/xmpcore/5.1.2

No actual source repository was found for this project.

As far as I can tell Adobe is not complying with their own license, because they aren't publishing a copyright, and
above links have gone dead...

Primary evidence that this code is BSD licensed comes from

https://mvnrepository.com/artifact/com.adobe.xmp/xmpcore/6.1.11

and the MANIFEST.MF file in the distributed jar file:

Manifest-Version: 1.0
Bnd-LastModified: 1605616735377
Build-Jdk-Spec: 1.8
Bundle-Description: The Adobe XMP Core library
Bundle-License: https://opensource.org/licenses/BSD-3-Clause
Bundle-ManifestVersion: 2
Bundle-Name: Adobe XMPCore
Bundle-SymbolicName: com.adobe.xmp.xmpcore
Bundle-Vendor: Adobe Systems Incorporated
Bundle-Version: 6.1.11
Created-By: Apache Maven Bundle Plugin
Export-Package: com.adobe.internal.xmp;uses:="com.adobe.internal.xmp.o
 ptions,com.adobe.internal.xmp.properties";version="6.1.11",com.adobe.
 internal.xmp.options;uses:="com.adobe.internal.xmp";version="6.1.11",
 com.adobe.internal.xmp.properties;uses:="com.adobe.internal.xmp.optio
 ns";version="6.1.11",com.adobe.internal.xmp.utils;uses:="com.adobe.in
 ternal.xmp.options";version="6.1.11",com.adobe.xmp.core.map;version="
 6.1.11"
Import-Package: javax.xml.parsers,org.w3c.dom,org.xml.sax
Require-Capability: osgi.ee;filter:="(&(osgi.ee=JavaSE)(version=1.6))"
Tool: Bnd-5.1.1.202006162103


Copy of License
---------------
A copy of the BSD-3-Clause license can be found at ../BSD-3-Clause.txt
relative to this file.