package wikielastic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikielastic.temp.ArticleDb;
import wikielastic.temp.MergedTmpReader;
import wikielastic.temp.RedirectDb;
import wikielastic.wiki.MergedWikiPage;
import wikielastic.wiki.WikiPage;
import wikielastic.wiki.WikiParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class Processor implements Runnable {
    private boolean tmpPhaseEnabled = true;
    private boolean mergedPhaseEnabled = true;
    private final static Logger logger = LoggerFactory.getLogger(Processor.class);
    private final static WikiPage streamEnd = new WikiPage();

    public final BlockingQueue<WikiPage> redirectsQueue = new ArrayBlockingQueue<>(128);
    // Larger queue size here, these are always the bottleneck. Redirects will likely never block (very fast to write)
    public final BlockingQueue<WikiPage> articlesQueue = new ArrayBlockingQueue<>(256);
    public final BlockingQueue<MergedWikiPage[]> elasticQueue = new ArrayBlockingQueue<>(3);

    public String filename;
    private final ArticleDb articleDb = new ArticleDb("tmp-articles");
    private final RedirectDb redirectDb = new RedirectDb("tmp-redirects");

    private final ProcessorStats processorStats = new ProcessorStats(this);

    public Processor(String filename, boolean tmpPhaseEnabled, boolean mergedPhaseEnabled) {
        this.filename = filename;
        this.tmpPhaseEnabled = tmpPhaseEnabled;
        this.mergedPhaseEnabled = mergedPhaseEnabled;
    }

    public static void process(String filename, boolean tmpPhaseEnabled, boolean mergedPhaseEnabled) {
        Processor processor = new Processor(filename, tmpPhaseEnabled, mergedPhaseEnabled);
        processor.run();
    }


    @Override
    public void run() {
        processorStats.startProcessorStatsPrinting();

        if (tmpPhaseEnabled) {
            logger.info("Beginning Tmp Processing");
            executeTmpPhase();
        }

        if (mergedPhaseEnabled) {
            logger.info("Beginning Elastic Processing");
            processMerged();
        }

        processorStats.stopProcessorStatsPrinting();
    }

    private List<Thread> executeTmpPhase() {
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

        List<Thread> threads = Arrays.asList(xmlThread, redirectThread, articleThread);
        processorStats.signalTmpStart();
        threads.stream().forEach(Thread::start);
        threads.stream().forEach(t -> {
            try {
                t.join();
            } catch (InterruptedException e) {
                logger.error("Thread interrupted: " + t.getName(), e);
                System.exit(1);
            }
        });
        processorStats.signalTmpEnd();
        return threads;
    }

    private void readXmlToQueues() {
        StreamHandler streamHandler = new StreamHandler<WikiPage>() {
            @Override
            public void handleItem(WikiPage page) {
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

        WikiParser.parse(streamHandler, filename);
    }

    class TmpMergedStreamHandler implements StreamHandler<MergedWikiPage> {
        private final int batchMax;
        private final ElasticWriter elasticWriter;
        private final List<MergedWikiPage> batch;
        BlockingQueue<MergedWikiPage[]> queue = new ArrayBlockingQueue<>(3);
        public final MergedWikiPage[] batchStreamEnd = new MergedWikiPage[1];

        TmpMergedStreamHandler(int batchMax, ElasticWriter elasticWriter) {
            this.queue = elasticQueue;
            this.batchMax = batchMax;
            this.elasticWriter = elasticWriter;
            this.batch = new ArrayList<>(batchMax);
        }

        private void queueCurrentBatch() {
            logger.info("Queueing batch");
            MergedWikiPage[] arr = new MergedWikiPage[batch.size()];
            batch.toArray(arr);
            try {
                queue.put(arr);
            } catch (InterruptedException e) {
                logger.error("Interrupted attempting to queue a new batch!", e);
                System.exit(1);
            }
            batch.clear();
        }

        @Override
        public void handleItem(MergedWikiPage page) {
            batch.add(page);

            if (batch.size() == batchMax) {
                queueCurrentBatch();
            }
        }

        @Override
        public void handleEnd() {
            queueCurrentBatch();
            try {
                queue.put(this.batchStreamEnd);
            } catch (InterruptedException e) {
                logger.error("Could not end stream!", e);
                System.exit(1);
            }
        }
    }

    public void processMerged() {
        ElasticWriter elasticWriter = new ElasticWriter("en-wikipedia");
        elasticWriter.setupIndex();
        MergedTmpReader mergedTmpReader = new MergedTmpReader(articleDb, redirectDb);

        TmpMergedStreamHandler tmpMergedStreamHandler = new TmpMergedStreamHandler(512, elasticWriter);

        processorStats.elasticStartedAt = System.currentTimeMillis();

        Thread tmpReader = new Thread(new Runnable() {
            @Override
            public void run() {
                mergedTmpReader.processPages(tmpMergedStreamHandler);
            }
        });
        tmpReader.setName("TmpBatchReader");
        tmpReader.start();

        logger.info("Will  begin writing to elasticsearch");

        processQueue(tmpMergedStreamHandler.queue, tmpMergedStreamHandler.batchStreamEnd, new StreamHandler<MergedWikiPage[]>() {
            @Override
            public void handleItem(MergedWikiPage[] batch) {
                logger.info("Handling...");
                elasticWriter.write(Arrays.asList(batch));
                processorStats.elasticItemsWritten.addAndGet(batch.length);
            }

            @Override
            public void handleEnd() {
                logger.info("Done writing to elasticsearch!");
                processorStats.elasticEndedAt = System.currentTimeMillis();
                elasticWriter.close();
            }
        });
    }

    public void processArticles() {
        try {
            articleDb.initializeGenerator();
        } catch (IOException e) {
            logger.error("Could not open articles temp file", e);
            System.exit(1);
        }

        processQueue(articlesQueue, this.streamEnd, new StreamHandler<WikiPage>() {
            @Override
            public void handleItem(WikiPage page) {
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
        processQueue(redirectsQueue, this.streamEnd, new StreamHandler<WikiPage>() {
            @Override
            public void handleItem(WikiPage page) {
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

    private <T> void processQueue(BlockingQueue<T> queue, T streamEndObject, StreamHandler<T> handler) {
        T item;
        try {
            while((item = queue.take()) != streamEndObject) {
                handler.handleItem(item);
            }
            handler.handleEnd();
        } catch (InterruptedException e) {
            logger.debug("Interrupted while waiting for new redirect", e);
        }
    }

}
