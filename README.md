# apache-drill-jdbc-plugin
A JDBC plugin for Apache Drill

Download Apache Drill 1.1 branch (from github)
Add the code to contrib and replace existing pom file with the pom file in this folder.

Build with mvn.
To just build the package use notations similar to following:
OSX: /Applications/apache-maven-3.2.5/bin/mvn clean install -pl contrib/storage-mpjdbc -DskipTests
LINUX: /usr/local/apache-maven-3.3.3/bin/mvn clean install -pl contrib/storage-mpjdbc -DskipTests

Take the jar in contrib/storage-mpjdbc/target and move it to jar folder of a drill dist. 
Add your jdbc driver to same folder.

Restart drillbits and conf a new plug-in and put in { type : "jdbc"}
and update, and you will get all configuration options.

Fill them in and update. Make sure not to use a dbname as name for the plug-in.
Example:

{
  "type": "jdbc",
  "driver": "com.mysql.jdbc.Driver",
  "uri": "jdbc:mysql://192.168.1.91/employees",
  "username": "maprdemo", 
  "password": "maprdemo",
  "enabled": true
}
  

Now you should be able to query the db using plug-in.table notation.
Dep on db you might need to have more or less detail in the URL. I have tried with Mysql and Netezza and Netezza demands dbname to be part of URL whilst Mysql doesn't.


To debug drill:

Add:
-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=y,address=8000
before -cp in the sqlline client script.

And hook up Eclipse for instance on port 8000.

/Magnus
