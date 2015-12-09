import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.util.Map;

public class RelevancyBolt extends BaseRichBolt {

    private OutputCollector collector;

    private RelevancyFilter filter;

    public RelevancyBolt(RelevancyFilter filter) {
        this.filter = filter;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("tweet");
        Double relevancyScore = filter.getRelevance(tweet);
        Values values = new Values();
        values.add(tuple.getLongByField("timestamp"));
        values.add(tweet);
        values.add(relevancyScore);
        collector.emit(values);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        String[] schema = { "timestamp", "tweet", "relevancy" };
        outputFieldsDeclarer.declare(new Fields(schema));
    }
}
