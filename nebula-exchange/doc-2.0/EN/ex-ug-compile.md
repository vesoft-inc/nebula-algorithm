# Compile Exchange

To compile Exchange, follow these steps:

1. Run these commands to install Nebula Java Client v2.x.

   ```bash
   $ git clone https://github.com/vesoft-inc/nebula-java.git
   $ cd nebula-java
   $ mvn clean install -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
   ```

   > **NOTE**: After the installation, you can see the `/com/vesoft/client/2.0.0-beta/client-2.0.0-beta.jar` in your local Maven repository.

2. Run these commands to package Nebula Exchange v2.x.

   ```bash
   $ git clone https://github.com/vesoft-inc/nebula-spark-utils.git
   $ cd nebula-spark-utils/nebula-exchange
   $ mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
   ```

After the compiling, you can see the structure of the Exchange directory as follows.

```text
.
├── README-CN.md
├── README.md
├── pom.xml
├── src
│   ├── main
│   └── test
└── target
    ├── classes
    ├── classes.timestamp
    ├── maven-archiver
    ├── nebula-exchange-2.x.y-javadoc.jar
    ├── nebula-exchange-2.x.y-sources.jar
    ├── nebula-exchange-2.x.y.jar
    ├── original-nebula-exchange-2.x.y.jar
    └── site
```

In the `target` directory, you can see the `exchange-2.x.y.jar` file.
> **NOTE**: The version of the JAR file depends on the releases of Nebula Java Client. You can find the latest versions on the [Releases page of the nebula-spark-utils repository](https://github.com/vesoft-inc/nebula-spark-utils "Click to go to GitHub").

To import data, you can refer to the example configuration in the `target/classes/application.conf` files.
