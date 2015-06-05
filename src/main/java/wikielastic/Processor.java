package wikielastic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.temp.ArticleDb;
import wikielastic.temp.RedirectDb;
import wikielastic.wiki.WikiPage;
import wikielastic.wiki.WikiPageHandler;
import wikielastic.wiki.WikiParser;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class Processor implements Runnable {
    private final static Logger logger = LoggerFactory.getLogger(Processor.class);
    private final static WikiPage streamEnd = new WikiPage();

    public final BlockingQueue<WikiPage> redirectsQueue = new ArrayBlockingQueue<>(128);
    // Larger queue size here, these are always the bottleneck. Redirects will likely never block (very fast to write)
    public final BlockingQueue<WikiPage> articlesQueue = new ArrayBlockingQueue<>(256);
    public String filename;
    private final RedirectDb redirectDb = new RedirectDb();

    private final ProcessorStats processorStats = new ProcessorStats(this);

    public Processor(String filename) {
        this.filename = filename;
    }

    public static void process(String filename) {
        Processor processor = new Processor(filename);
        processor.run();
    }


    @Override
    public void run() {
        Thread xmlThread = new Thread(new Runnable() {
            @Override
            public void run() {
                readXmlToQueues();
            }
        });
       xmlThread.setName("originXMLParser");

        Thread redirectThread = new Thread(new Runnable() {
            @Override
            public void run() {
                processRedirects();
            }
        });
        redirectThread.setName("tmpRedirectWriter");

        Thread articleThread = new Thread(new Runnable() {
            @Override
            public void run() {
                processArticles();
            }
        });
        articleThread.setName("tmpArticleWriter");

        try {
            processorStats.startProcessorStatsPrinting();

            processorStats.signalTmpStart();

            xmlThread.start();
            redirectThread.start();
            articleThread.start();

            processorStats.signalTmpEnd();

            xmlThread.join();
            redirectThread.join();
            articleThread.join();

            processorStats.stopProcessorStatsPrinting();
        } catch (InterruptedException e) {
            logger.error("Interrupted during main execution!", e);
            System.exit(1);
        }
    }

    private void readXmlToQueues() {
        WikiPageHandler wikiPageHandler = new WikiPageHandler() {
            @Override
            public void handlePage(WikiPage page) {
                processorStats.rawPagesProcessed.incrementAndGet();

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
                logger.info("Finished reading XML");
            }
        };

        WikiParser.parse(wikiPageHandler, filename);
    }

    public void processArticles() {
        ArticleDb articleDb = new ArticleDb("tmp-articles");
        try {
            articleDb.initializeGenerator();
        } catch (IOException e) {
            logger.error("Could not open articles temp file", e);
            System.exit(1);
        }

        processQueue(articlesQueue, new WikiPageHandler() {
            @Override
            public void handlePage(WikiPage page) {
                try {
                    articleDb.writePage(page);
                    processorStats.tmpArticlesProcessed.incrementAndGet();
                } catch (IOException e) {
                    logger.error("Could not write article", e);
                    System.exit(1);
                }
            }

            @Override
            public void handleEnd() {
                try {
                    articleDb.closeGenerator();
                } catch (IOException e) {
                    logger.error("Could not close articledb", e);
                }
                logger.info("Finished loading articles into temp db");
            }
        });
    }

    private void processRedirects() {
        processQueue(redirectsQueue, new WikiPageHandler() {
            @Override
            public void handlePage(WikiPage page) {
                try {
                    redirectDb.writeRedirect(page);
                    processorStats.tmpRedirectsProcessed.incrementAndGet();
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
                if (!handler.equals(streamEnd)) {
                    handler.handlePage(page);
                }
            }
            handler.handleEnd();
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for new redirect", e);
        }
    }

}
