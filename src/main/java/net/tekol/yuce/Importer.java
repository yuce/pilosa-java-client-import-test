package net.tekol.yuce;

import com.pilosa.client.Bit;
import com.pilosa.client.BitIterator;
import com.pilosa.client.PilosaClient;
import com.pilosa.client.orm.Frame;
import com.pilosa.client.orm.Index;
import com.pilosa.client.orm.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class Importer {
    public static void main(String...args) {
        if (args.length != 2) {
            System.err.println("Usage: %prog pilosa_addr max_id");
            System.exit(1);
        }

        for (String arg : args) {
            logger.debug("ARG: " + arg);
        }
        String pilosaAddr = args[0];
        long maxID = Long.parseLong(args[1]);

        PilosaClient client = PilosaClient.withAddress(pilosaAddr);
        Schema schema = client.readSchema();
        Index index = schema.index("i1");
        Frame frame = index.frame("f1");
        client.syncSchema(schema);

        logger.info("maxID: " + maxID);
        BitIterator iterator = new XBitIterator(maxID);
        long tic = System.nanoTime();
        client.importFrame(frame, iterator);
        long tac = System.nanoTime();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(tac - tic);
        logger.info(String.format("Took %d ms", elapsedMs));
    }

    private final static Logger logger = LoggerFactory.getLogger(Importer.class);
}

class XBitIterator implements BitIterator {

    XBitIterator(long maxID) {
        this.maxID = maxID;
    }

    public boolean hasNext() {
        return this.currentID <= this.maxID;
    }

    public Bit next() {
        long id = this.currentID++;
        return Bit.create(id, id);
    }

    private long maxID;
    private long currentID = 0;
}