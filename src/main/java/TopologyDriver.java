import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import java.io.File;

public class TopologyDriver {

    static String[] tweetSchema = { "tweet-contributors-list",
            "date-created" ,
            "current-user-retweet-id" ,
            "favourite-count" ,
            "geolocation-latitude,geolocation-longitude" ,
            "user-id" ,
            "in-reply-to-screen-name" ,
            "in-reply-to-status-id" ,
            "in-reply-to-user-id" ,
            "language" ,
            "place-fullname" ,
            "quoted-status-id" ,
            "retweet-count" ,
            "source" ,
            "text" ,
            "username" ,
            "userid" ,
            "user-favourites-count" ,
            "user-followers-count" ,
            "user-friends-count" ,
            "status-isfavourited" ,
            "status-is-possibly-sensitive" ,
            "status-is-retweet" ,
            "status-is-retweeted" ,
            "status-is-truncated" };

    static String[] projectedSchema = { "date-created", "text" };

    public static void main(String[] args) {
        int bufferLength = 75000;
        Integer k = 10;
        Boolean batch = true;
        TopologyBuilder builder = new TopologyBuilder();
        Config conf = new Config();
        String[] keywords = { "the","i","to","a","and","is","in","it","you","of","for","on","my","\\'s","that","at",
                "with","me","do","have","just","this","be","n\\'t","so","are","\\'m","not","was","but","out","up",
                "what","now","new","from","your" };
        Double radius = Double.valueOf(0.3);
        TweetFileProducer producer = new TweetFileProducer("data" + File.separator + "tweet_file.txt");
        builder.setSpout("source", new TweetSpout(producer), 1).setNumTasks(1);
        RelevancyFilter relevancyFilter = new RelevancyFilter(keywords);
        builder.setBolt("relevancy", new RelevancyBolt(relevancyFilter), 1).setNumTasks(1).shuffleGrouping("source");
        DiversityOperator diversityOperator = new DiversityOperator(k, radius, batch, bufferLength);
        builder.setBolt("diversity", new DiversityBolt(diversityOperator), 1).setNumTasks(1).shuffleGrouping("relevancy");
        conf.setDebug(false);
        conf.setNumWorkers(1);
        conf.setNumAckers(1);
        conf.put(Config.TOPOLOGY_WORKER_CHILDOPTS,
                "-Xmx4096m -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:NewSize=128m " +
                        "-XX:CMSInitiatingOccupancyFraction=70 -XX:-CMSConcurrentMTEnabled -Djava.net.preferIPv4Stack=true"
        );
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("streamdiv", conf, builder.createTopology());
        Utils.sleep(30000);
        localCluster.killTopology("streamdiv");
        localCluster.shutdown();
    }
}
