import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

public class DiversityBolt extends BaseRichBolt {

    private OutputCollector collector;

    private int k;

    private DiversityOperator operator;

    public DiversityBolt(DiversityOperator operator) {
        this.operator = operator;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("tweet");
        Long timestamp = tuple.getLongByField("timestamp");
        Double relevancyScore = tuple.getDoubleByField("relevancy");
        /**
         * call the diversity operator
         */
        Values result = operator.execute(tweet, timestamp, relevancyScore);
        if (result != null)
            collector.emit(result);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        String[] schema = new String[operator.getK()];
        for (int i = 0; i < k; i++) {
            schema[i] = Integer.toString(i);
        }
        outputFieldsDeclarer.declare(new Fields(schema));
    }
}
