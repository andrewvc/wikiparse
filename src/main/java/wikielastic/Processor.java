package wikielastic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.temp.RedirectDb;
import wikielastic.wiki.WikiPage;
import wikielastic.wiki.WikiPageHandler;
import wikielastic.wiki.WikiParser;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class Processor implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(Processor.class);
    private final static WikiPage streamEnd = new WikiPage();
    private final BlockingQueue<WikiPage> redirectsQueue = new ArrayBlockingQueue<>(4096);
    // Smaller queue size here because these take up a lot more memory
    private final BlockingQueue<WikiPage> articlesQueue = new ArrayBlockingQueue<>(256);
    public final String filename;
    private final RedirectDb redirectDb = new RedirectDb();

    public static void process(String filename) {
        Processor processor = new Processor(filename);
        processor.run();
    }

    public Processor(String filename) {
        this.filename = filename;
    }

    @Override
    public void run() {
        readXmlToQueues();
        processRedirects();
        redirectDb.printRedirects();
    }

    private void readXmlToQueues() {
        WikiPageHandler wikiPageHandler = new WikiPageHandler() {
            @Override
            public void handlePage(WikiPage page) {
                if (page == null) {
                    logger.error("Unexpected null page! Cannot handle!");
                    System.exit(1);
                } else if (page.isRedirect()) {
                    try {
                        redirectsQueue.put(page);
                    } catch (InterruptedException e) {
                        logger.debug("Interrupted while waiting to put redirect on queue", e);
                    }
                } else {
                    try {
                        articlesQueue.put(page);
                    } catch (InterruptedException e) {
                        logger.debug("Interrupted while waiting to put article on queue", e);
                    }
                }
            }

            @Override
            public void handleEnd() {
                redirectsQueue.add(streamEnd);
                articlesQueue.add(streamEnd);
            }
        };

        WikiParser.parse(wikiPageHandler, filename);
    }

    private void processRedirects() {
        processQueue(redirectsQueue, new WikiPageHandler() {
            @Override
            public void handlePage(WikiPage page) {
                try {
                    redirectDb.writeRedirect(page);
                } catch (RedirectDb.InvalidPageTypeException e) {
                    logger.error("Encountered an invalid page!" + page.isRedirect(), e);
                }
            }

            @Override
            public void handleEnd() {
                logger.info("Finished processing redirects");
            }
        });
    }

    private void processQueue(BlockingQueue<WikiPage> queue, WikiPageHandler handler) {
        WikiPage page;
        try {
            while((page = queue.take()) != streamEnd) {
                if (handler != streamEnd) {
                    handler.handlePage(page);
                } else {
                    handler.handleEnd();
                }
            }
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for new redirect", e);
        }
    }

}
