package wikielastic.temp;

import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.StreamHandler;
import wikielastic.wiki.MergedWikiPage;

import java.io.IOException;

/**
 * Created by andrewcholakian on 6/5/15.
 */
public class MergedTmpReader {
    private final ArticleDb articleDb;
    private final RedirectDb redirectDb;
    public final static Logger logger = LoggerFactory.getLogger(MergedTmpReader.class);

    public MergedTmpReader(ArticleDb articleDb, RedirectDb redirectDb) {
        this.articleDb = articleDb;
        this.redirectDb = redirectDb;
    }

    public void processPages(StreamHandler<MergedWikiPage> sh) {
        try {
            articleDb.parse(new StreamHandler<MergedWikiPage>() {
                @Override
                public void handleItem(MergedWikiPage page) {
                    try {
                        page.redirects = redirectDb.getRedirects(page.title);
                    } catch (RocksDBException e) {
                        logger.error("RocksDB Error getting redirects for: " + page, e);
                        System.exit(1);
                    } catch (IOException e) {
                        logger.error("IOException getting redirects for: " + page, e);
                        System.exit(1);
                    }

                    sh.handleItem(page);
                }

                @Override
                public void handleEnd() {
                    sh.handleEnd();
                }
            });
        } catch (IOException e) {
            logger.error("MergedTmpReader Could not parse articleDb", e);
            System.exit(1);
        }

    }
}
