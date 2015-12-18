import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import java.util.List;
import java.util.Map;

public class DiversityBolt extends BaseRichBolt {

    private OutputCollector collector;

    private DiversityOperator operator;

    private DescriptiveStatistics distanceStatistics;

    private DescriptiveStatistics relevancyStatistics;

    private DescriptiveStatistics intensityStatistics;

    public DiversityBolt(DiversityOperator operator) {
        this.operator = operator;
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        distanceStatistics = new DescriptiveStatistics();
        relevancyStatistics = new DescriptiveStatistics();
        intensityStatistics = new DescriptiveStatistics();
    }

    @Override
    public void cleanup() {
        System.out.println("Mean Average Distance: " + operator.getAverageDistance());
        System.out.println("Mean Average Relevancy Score: " + operator.getAverageRelevancyScore());
        System.out.println("Mean Average Intensity Score: " + operator.getAverageIntensityScore());
    }

    @Override
    public void execute(Tuple tuple) {
        /**
         * call the diversity operator
         */
        List<Tuple> result = operator.execute(tuple);
        if (result != null) {
            Values topK = new Values();
            for (int i = 0; i < result.size(); i++) {
                topK.add(result.get(i).getStringByField("tweet"));
            }
            while (topK.size() < operator.getK()) {
                topK.add(new String(""));
            }
            distanceStatistics.addValue(operator.getAverageDistance());
            relevancyStatistics.addValue(operator.getAverageRelevancyScore());
            intensityStatistics.addValue(operator.getAverageIntensityScore());
            collector.emit(topK);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        String[] schema = new String[operator.getK()];
        for (int i = 0; i < operator.getK(); i++) {
            schema[i] = Integer.toString(i);
        }
        outputFieldsDeclarer.declare(new Fields(schema));
    }
}
