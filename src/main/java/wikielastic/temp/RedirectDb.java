package wikielastic.temp;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.wiki.WikiPage;

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



    public void writeRedirect(WikiPage page) throws InvalidPageTypeException {
        if (!page.isRedirect()) {
            throw new InvalidPageTypeException(String.format("Page %s is not a redirect!", page));
        }

        logger.info("Write redirect: " + page);

        try {
            byte[] pageBytes = objectMapper.writeValueAsBytes(page);
            db.put(page.redirect.getBytes(), pageBytes);
        } catch (JsonProcessingException e) {
            logger.error("Could map value for wikipage: " + page, e);
        } catch (RocksDBException e) {
            logger.error("Could not write redirect to DB: " + page, e);
        }
    }

    public class InvalidPageTypeException extends Exception {
        InvalidPageTypeException(String msg) {
            super(msg);
        }
    }
}
