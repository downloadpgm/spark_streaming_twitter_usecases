# twitter sbt

To run, prepare a Spark environment having YARN/Standalone cluster manager

1) download sbt

```shell
$ wget https://github.com/sbt/sbt/releases/download/v1.3.8/sbt-1.3.8.tgz
$ tar zxvf sbt-1.3.8.tgz
$ mv sbt /usr/local
$ export PATH=$PATH:/usr/local/sbt/bin
```

2) run sbt to prepare enviroment

```shell
$ sbt
```

3) create directory for build

```shell
$ mkdir app
$ cd app
$ # copy twitter-consumer.sbt and TweetStream.scala
```

4) build and create jar
```shell
$ sbt package
$ cd ~
```

5) run the package

```shell
$ spark-submit --master yarn --packages org.apache.bahir:spark-streaming-twitter_2.11:2.3.2 --class TweetStream.tweetstream app/target/scala-2.11/twitter-consumer_2.11-1.0.0.jar

or 

$ spark-submit --master spark://<spk-mst>:7077 --packages org.apache.bahir:spark-streaming-twitter_2.11:2.3.2 --class TweetStream.tweetstream app/target/scala-2.11/twitter-consumer_2.11-1.0.0.jar
```



