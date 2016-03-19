
# Running

To run the ingest node use the following command line. 

java -jar build/libs/ingest-node.jar 

This will print usage info. This jar contains all dependencies, and thus can be copied to any machine and run
without any additional setup. It will create &lt;user_home_dir&gt;/.jj and place cassandra related files there.

Some of the startup spam can be reduced by adding -Done-jar.silent=true

https://sourceforge.net/p/one-jar/bugs/69/

You can omit the onejar property if you don't mind some additional spam.

