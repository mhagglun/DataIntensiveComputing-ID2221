## Usage

First start the HDFS NameNode and DataNode
```bash
$HADOOP_HOME/bin/hdfs --daemon start namenode
$HADOOP_HOME/bin/hdfs --daemon start datanode
```

Then create a folder named `input` in HDFS and upload the files to it.
```bash
$HADOOP_HOME/bin/hdfs dfs -mkdir -p topten_input
$HADOOP_HOME/bin/hdfs dfs -put users.xml topten_input/users.xml
$HADOOP_HOME/bin/hdfs dfs -ls topten_input
```

Next we start HBase and the HBase shell
```bash
$HBASE_HOME/bin/start-hbase.sh
$HBASE_HOME/bin/hbase shell
```

In the HBase shell we then create a new table `topten` with one column family named `info` which we'll later use to store the id and reputation of users.
```bash
create 'topten', 'info'
```

Set the HADOOP_CLASSPATH environment variable
```bash
export HADOOP_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
export HBASE_CLASSPATH=$($HBASE_HOME/bin/hbase classpath)
export HADOOP_CLASSPATH=$HADOOP_CLASSPATH:$HBASE_CLASSPATH
```


Compile the code into a target directory and make the jar file.
```bash
javac -cp $HADOOP_CLASSPATH -d topten_classes topten/TopTen.java
jar -cvf topten.jar -C topten_classes/ .
```

We're now ready to run the application using the following command

```bash
$HADOOP_HOME/bin/hadoop jar topten.jar id2221.topten.TopTen topten_input topten_output
```

The output may be viewed by executing the following command inside the HBase shell.
```bash
scan 'topten'
```