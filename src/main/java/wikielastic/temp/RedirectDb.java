package wikielastic.temp;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.wiki.WikiPage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class RedirectDb {
    String filename;
    RocksDB db = null;
    ObjectMapper objectMapper = new ObjectMapper();
    public static Logger logger = LoggerFactory.getLogger(RedirectDb.class);

    public RedirectDb() {
        this.filename = "tmp-redirects-" + UUID.randomUUID().toString();

        RocksDB.loadLibrary();
        Options options = new Options().setCreateIfMissing(true);

        try {
            this.db = RocksDB.open(options, filename);
        } catch (RocksDBException e) {
            logger.error("Could not open rocksdb", e);
            System.exit(1);
        }
    }

    public void printRedirects() {
        RocksIterator iter = db.newIterator();
        for (iter.seekToFirst(); iter.isValid(); iter.next()) {
            logger.info(new String(iter.key()) + " -> " + new String(iter.value()));
        }
    }

    public void writeRedirect(WikiPage page) throws InvalidPageTypeException {
        if (!page.isRedirect()) {
            throw new InvalidPageTypeException(String.format("Page %s is not a redirect!", page));
        }

        logger.info("Write redirect: " + page);

        try {
            byte[] key = page.redirect.getBytes();

            List<String> redirectList = getRedirects(key);
            redirectList.add(page.title);

            db.put(key, objectMapper.writeValueAsBytes(redirectList));
        } catch (JsonProcessingException e) {
            logger.error("Could not map value for wikipage: " + page, e);
            System.exit(1);
        } catch (RocksDBException e) {
            logger.error("Could not write redirect to DB: " + page, e);
            System.exit(1);
        } catch (IOException e) {
            logger.error("IO Exception writing redirect", e);
            System.exit(1);
        }
    }

    // Returns list of redirects, returns an empty list when key is missing!
    public List<String> getRedirects(byte[] key) throws RocksDBException, IOException {
        byte[] val = db.get(key);

        List<String> redirectList = null;

        if (val != null) {
            redirectList = objectMapper.readValue(val, List.class);
        } else {
            redirectList = new LinkedList<>();
        }

        return redirectList;
    }

    public class InvalidPageTypeException extends Exception {
        InvalidPageTypeException(String msg) {
            super(msg);
        }
    }
}
