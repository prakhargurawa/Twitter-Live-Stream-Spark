import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.twitter.*;
import org.apache.spark.streaming.api.java.*;
import twitter4j.*;
import java.util.Arrays;
import java.util.List;
import scala.Tuple2;
import java.util.stream.Collectors;

/*
@author : Prakhar Gurawa
 */
public class TwitterStreamApplication {
    public static void main(String[] args) throws Exception {
        /*
            Reference:
                http://spark.incubator.apache.org/docs/latest/streaming-programming-guide.html
                http://spark.apache.org/docs/latest/index.html
                https://developer.twitter.com
                http://ampcamp.berkeley.edu/big-data-mini-course/realtime-processing-with-spark-streaming.html
         */

        // Solution 1
        // Setup parallelism, hadoop directory and memory allocation limits : 4 cores of processor and 1 Gigabyte of memory
        System.setProperty("hadoop.home.dir", "C:/winutils");
        SparkConf sparkConf = new SparkConf()
                .setAppName("Sentiment Analysis using SVM")
                .setMaster("local[4]").set("spark.executor.memory", "1g");

        // Setting properties from twitter account to establish connection and receive the twitter feed
        System.setProperty("twitter4j.oauth.consumerKey", "UoponOMW5IVqmcILnraGXtDTg");
        System.setProperty("twitter4j.oauth.consumerSecret", "DkAmRtgbrocEtnFAIFOeHybOqRB0rZzwUCRC5uP1BcWDVKkylR");
        System.setProperty("twitter4j.oauth.accessToken", "2922636842-yUnidrkaCL14lY9b7Wnnbpd3PcZEOp31OFtjjoN");
        System.setProperty("twitter4j.oauth.accessTokenSecret", "Jp1y5IsquZwAGadpksnzHVULnquj1jvOA7NkNd47wRMAt");

        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, new Duration(2000));

        // Establishing connection with Twitter stream : https://developer.twitter.com/en/portal/dashboard
        JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc);
        // tweets.print();

        // Printing 10 tweets received from Twitter stream
        JavaDStream<String> statuses = tweets.map(status -> status.getText());
        statuses.print();

        // Printing number of character in a tweet (excluding spaces)
        JavaDStream<String> tweetCountChars = statuses.map(s -> "Tweet ==> "+s+" Character Count ==> "+ Utility.countCharacters(s));
        tweetCountChars.print();

        //Printing number of words in a tweet
        JavaDStream<String> tweetCountWords = statuses.map(s -> "Tweet ==> "+s+" Word Count ==> "+s.split(" ").length);
        tweetCountWords.print();

        //Printing hashtags in a tweet
        JavaPairDStream<String,List<String>> tweetHashTags = statuses.mapToPair(s -> new Tuple2<String,List<String>>(
           "Tweet ==> "+s+" Hashtags ==> ",Arrays.asList(s.split(" ")).stream()
                .filter(p -> p.startsWith("#"))
                .collect(Collectors.toList())
        ));
        tweetHashTags.print();

        // Average Number of characters and words per tweet
        statuses.foreachRDD(s->{
            long tweetCounterWords = s.map(x -> x.split(" ").length).fold(0, Integer::sum);
            long tweetCounterChar = s.map(x ->  Utility.countCharacters(x)).fold(0, Integer::sum);
            long tweetCounter = s.count()==0? 1:s.count();
            System.out.println("Average number of character : "+tweetCounterChar/tweetCounter+" and average number of words : "+tweetCounterWords/tweetCounter+" per tweet");
        });

        // Top 10 hash tags based on their number of occurrence
        // Reduce Function for sum Reference : https://backtobazics.com/big-data/spark/apache-spark-reducebykey-example/
        Function2<Integer, Integer, Integer> reduceSumFunc = (acc, n) -> (acc + n);
        // Using flatmap to get all hashtags
        JavaPairDStream<String,Integer> tweetPairs = tweetHashTags.map(tuple -> tuple._2())
                .flatMap(x -> x.iterator())
                .mapToPair(t-> new Tuple2<String,Integer>(t,1));
        // Following map-reduce pattern
        JavaPairDStream<Integer,String> swappedTweetPairs = tweetPairs.reduceByKey(reduceSumFunc).mapToPair(Tuple2::swap);
        swappedTweetPairs.foreachRDD(rdd->{
            if(!rdd.isEmpty()){
                // Reference : http://ampcamp.berkeley.edu/big-data-mini-course/realtime-processing-with-spark-streaming.html
                String out = "\nTop 10 hashtags:\n";
                for (Tuple2<Integer, String> t: rdd.sortByKey(false).take(10)) {
                    out = out + t.toString() + "\n";
                }
                System.out.println(out);
            }
        });

        /*
          Sample Output of top 10 hashtags:
                Top 10 hashtags:
                (7,#ShopeeMakeMyDay)
                (1,#PresidentElectBiden)
                (1,#instagraffiti)
                (1,#สกินแคร์มือสอง)
                (1,#legs)
                (1,#HadesGame)
                (1,#FreshAsMrPEntry)
                (1,#ShopeeMakeMyDay
                SAYANG)
                (1,#twik)
                (1,#gymlife)
         */

        // Repeat the above two operations for last 5 min og tweets, computing every 30 sec
        // every time the window slides over a source DStream, the source RDDs that fall within the window are combined and operated upon to produce the RDDs of the windowed DStream
        statuses.window(new Duration(5 * 1000 * 60), new Duration((1000 * 30))).foreachRDD(
                s->{
                    long tweetCounterWords = s.map(x -> x.split(" ").length).fold(0, Integer::sum);
                    long tweetCounterChar = s.map(x ->  Utility.countCharacters(x)).fold(0, Integer::sum);
                    long tweetCounter = s.count()==0? 1:s.count();
                    System.out.println("Window range Average number of character : "+tweetCounterChar/tweetCounter+" and average number of words : "+tweetCounterWords/tweetCounter+" per tweet");
                });

        tweetPairs.reduceByKeyAndWindow(reduceSumFunc, new Duration(5 * 1000 * 60), new Duration(30 * 1000))
                .mapToPair(Tuple2::swap)
                .foreachRDD(rdd->{
                    if(!rdd.isEmpty()){
                        // Reference : http://ampcamp.berkeley.edu/big-data-mini-course/realtime-processing-with-spark-streaming.html
                        String out = "\nTop 10 hashtags Window ranges :\n";
                        for (Tuple2<Integer, String> t: rdd.sortByKey(false).take(10)) {
                            out = out + t.toString() + "\n";
                        }
                        System.out.println(out);
                    }
                });

        // Uncomment for Checkpointing
        // jssc.checkpoint("D:/TwitterLogs");
        // Start the twitter stream
        jssc.start();
        // Wait for termination
        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
