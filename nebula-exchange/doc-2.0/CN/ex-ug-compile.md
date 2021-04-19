# 编译Exchange

本文介绍如何编译Nebula Exchange。您也可以直接[下载](https://repo1.maven.org/maven2/com/vesoft/nebula-exchange/2.0.0/)编译完成的`.jar`文件。

## 前提条件

安装[Maven](https://maven.apache.org/download.cgi)。

## 编译流程

1. 克隆仓库`nebula-java`。

   ```bash
   git clone -b v2.0.0-ga https://github.com/vesoft-inc/nebula-java.git
   ```

2. 切换到目录`nebula-java`。

   ```bash
   cd nebula-java
   ```

3. 安装Nebula Java Client 2.0.0。

   ```bash
   mvn clean install -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
   ```

   >**说明**：安装后在本地Maven仓库会生成`.jar`文件，例如`com/vesoft/client/2.0.0/client-2.0.0.jar`。

4. 返回根目录克隆仓库`nebula-spark-utils`。

   ```bash
   cd ~ && git clone -b v2.0.0 https://github.com/vesoft-inc/nebula-spark-utils.git
   ```

5. 切换到目录`nebula-exchange`。

   ```bash
   cd nebula-spark-utils/nebula-exchange
   ```

6. 打包Nebula Exchange 2.0.0。

   ```bash
   mvn clean package -Dmaven.test.skip=true -Dgpg.skip -Dmaven.javadoc.skip=true
   ```

   >**说明**：如果报错`Could not resolve dependencies for project xxx`，请修改Maven安装目录下`libexec/conf/settings.xml`文件的`mirror`部分：
   >
   >```text
   ><mirror>
   ><id>alimaven</id>
   ><mirrorOf>central</mirrorOf>
   ><name>aliyun maven</name>
   ><url>http://maven.aliyun.com/nexus/content/repositories/central/</url>
   ></mirror>
   >```

编译成功后，您可以在当前目录里查看到类似如下目录结构。

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

在`target`目录下，您可以找到`exchange-2.x.y.jar`文件。

> **说明**：`.jar`文件版本号会因Nebula Java Client的发布版本而变化。您可以在[Releases页面](https://github.com/vesoft-inc/nebula-java/releases)查看最新版本。

迁移数据时，您可以参考配置文件[`target/classes/application.conf`](https://github.com/vesoft-inc/nebula-spark-utils/blob/master/nebula-exchange/src/main/resources/application.conf)。
