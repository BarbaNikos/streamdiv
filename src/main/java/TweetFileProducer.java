import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.*;

/**
 * Created by nick on 12/9/15.
 */
public class TweetFileProducer {

    Logger logger = LoggerFactory.getLogger(TweetFileProducer.class);

    private Fields schema;

    private Fields projectedSchema;

    private String pathToFile;

    private BufferedReader reader;

    private boolean finished;

    public TweetFileProducer(String pathToFile, String[] schema, String[] projectedSchema) {
        this.schema = new Fields(schema);
        this.projectedSchema = new Fields(projectedSchema);
        this.pathToFile = pathToFile;
        finished = false;
    }

    public void init() {
        logger.info("initializing input for file: " + pathToFile);
        File input = new File(pathToFile);
        if (input.exists() && input.isFile()) {
            try {
                reader = new BufferedReader(new FileReader(input));
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }else {
            logger.error("file not found");
        }
        finished = false;
    }

    public Values nextTuple() {
        if (finished)
            return null;
        Values values = new Values();
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (line != null) {
            String[] attributes = line.split("\\|");
            if (attributes.length < schema.size())
                return null;
            for (int i = 0; i < schema.size(); i++) {
                if (projectedSchema.toList().contains(schema.get(i)))
                    values.add(attributes[i]);
            }
            return values;
        }else {
            finished = true;
            return null;
        }
    }

}
