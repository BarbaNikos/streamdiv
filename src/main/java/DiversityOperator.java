import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class DiversityOperator implements Serializable {

    private int k;

    private ArrayList<Tuple> topK;

    private ArrayList<Tuple> buffer;

    private int bufLen;

    private double radius;

    private boolean batch;

    public DiversityOperator(int k, double radius, boolean batch) {
        this.k = k;
        topK = new ArrayList<>();
        this.radius = radius;
        this.batch = batch;
        this.tweetBuf = new ArrayList<>();
        this.timestampBuf = new ArrayList<>();
        this.relevanceBuf = new ArrayList<>();
        this.bufLen = 10;
    }

    public List<Tuple> execute(Tuple tuple) {
        if(batch){
            String tweet = tuple.getStringByField("tweet");
            Long timestamp = tuple.getLongByField("timestamp");
            Double relevancyScore = tuple.getDoubleByField("relevancy");
            tweetBuf.add(tweet);
            timestampBuf.add(timestamp);
            relevanceBuf.add(relevancyScore);
            if(tweetBuf.size()>=bufLen)
                return batchReplace();
            else
                return null;
        }
        else
            return incrementalReplace(tuple);
    }

    /**
     * TODO: To be implemented
     * This function is only called when the buffers get full so no need to include that check within the function.
     * Remember to empty the buffers before returning.
     */
    public List<Tuple> batchReplace() {
        int bufferSize = this.tweetBuf.size();
        for (int i = 0; i < bufferSize; i++) {
            this.topK = incrementalReplace(this.tweetBuf.get(i));
        }
        this.tweetBuf.clear();
        this.timestampBuf.clear();
        this.relevanceBuf.clear();
        return new ArrayList<Tuple>(this.topK);
    }

    /*
     * Decide whether to keep or discard a newly incoming tuple
     */

    public List<Tuple> incrementalReplace(Tuple tuple) {
        if (topK.size() < k) {
            topK.add(tuple);
        } else {
            ArrayList<Tuple> dupTuples = new ArrayList<>();
            for(int i = 0; i < k; i++) {
                Tuple temp = topK.get(i);
                if(dist(temp.getStringByField("tweet"),tuple.getStringByField("tweet")) > radius)
                    dupTuples.add(temp);
            }
            if (dupTuples.size() == 1) {
                Tuple temp = dupTuples.get(0);
                if(getCombinedScore(
                        tuple.getLongByField("timestamp"), true,
                        tuple.getDoubleByField("relevancy")) > getCombinedScore(temp.getLongByField("timestamp"),
                        true,temp.getDoubleByField("relevancy"))
                        ) {
                    topK.remove(temp);
                    topK.add(tuple);
                }
            } else if (dupTuples.size() > 1) {
                dupTuples.add(tuple);
                double minScore = Double.MAX_VALUE;
                Tuple minTuple = null;
                for(Tuple t : dupTuples) {
                    if(getCombinedScore(t.getLongByField("timestamp"),true,t.getDoubleByField("relevancy")) < minScore){
                        minScore = t.getDoubleByField("relevancy");
                        minTuple = t;
                    }
                }
                if (minTuple != null)
                    topK.remove(minTuple);
            }
        }
        return new ArrayList<>(topK);
    }

    /*
     * Get the cosine similarity between two twitters
     */

    private ArrayList<double[]> getVectors(String tweet1, String tweet2) {
        int vectorIdx = 0;
        ArrayList<double[]> ret = new ArrayList<>();
        String[] words1 = tweet1.split(" ");
        String[] words2 = tweet2.split(" ");
        HashMap<String,Integer> words = new HashMap<>();
        for(String s: words1) {
            if (!words.containsKey(s))
                words.put(s,vectorIdx++);
        }
        for(String s : words2) {
            if (!words.containsKey(s))
                words.put(s,vectorIdx++);
        }
        double[] v1 = new double[words.size()];
        double[] v2 = new double[words.size()];
        for (String s: words1)
            v1[words.get(s)] = v1[words.get(s)] + 1;
        for (String s : words2)
            v2[words.get(s)] = v2[words.get(s)] + 1;
        ret.add(v1);
        ret.add(v2);
        return ret;
    }

    public double dist(String tweet1, String tweet2) {
        ArrayList<double[]> vec = getVectors(tweet1, tweet2);
        double[] vectorA = vec.get(0);
        double[] vectorB = vec.get(1);
        double dotProduct = 0.0;
        double normA = 0.0;
        double normB = 0.0;
        for (int i = 0; i < vectorA.length; i++) {
            dotProduct += vectorA[i] * vectorB[i];
            normA += Math.pow(vectorA[i], 2);
            normB += Math.pow(vectorB[i], 2);
        }
        return dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    }

    /*
     * Get Recency score
     */

    public double getRecencyScore(long arrivalTime, boolean exponential) {
        long currentTime = System.nanoTime();
        if (exponential)
            return Math.exp(-1*Math.abs(arrivalTime-currentTime));
        return -1*Math.abs(arrivalTime-currentTime);
    }

    /*
     * Generating combined intensity score
     */
    public double getCombinedScore(long arrivalTime, boolean exponential, double relevancyScore) {
        double recencyScore = getRecencyScore(arrivalTime, exponential);
        double alpha = 0.5;
        return alpha * relevancyScore + (1-alpha) * recencyScore;
    }

    public int getK() {
        return k;
    }
}
