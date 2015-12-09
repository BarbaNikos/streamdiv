import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class TweetSpout extends BaseRichSpout {

    Logger logger = LoggerFactory.getLogger(TweetSpout.class);

    private SpoutOutputCollector spoutOutputCollector;

    private TweetFileProducer producer;

    public TweetSpout(TweetFileProducer producer) {
        this.producer = producer;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        String[] schema = { "timestamp", "tweet" };
        outputFieldsDeclarer.declare(new Fields(schema));
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        producer.init();
    }

    @Override
    public void nextTuple() {
        Values tuple = producer.nextTuple();
        if (tuple != null)
            spoutOutputCollector.emit(tuple);
    }

}
