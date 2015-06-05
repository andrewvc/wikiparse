package wikielastic;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class ProcessorStats {
    public final Processor processor;
    public volatile Long tmpStartedAt = null;
    public volatile Long tmpEndedAt = null;
    public volatile Long endedAt = null;
    public final AtomicLong rawPagesProcessed = new AtomicLong(0);
    public final AtomicLong tmpArticlesProcessed = new AtomicLong(0);
    public final AtomicLong tmpRedirectsProcessed = new AtomicLong(0);
    private final Timer processorStatsPrintTimer = new Timer();

    public ProcessorStats(Processor processor) {
        this.processor = processor;
    }

    public long signalTmpStart() {
        tmpStartedAt = System.currentTimeMillis();
        return tmpStartedAt;
    }

    public long signalTmpEnd() {
        tmpEndedAt = System.currentTimeMillis();
        return tmpEndedAt;
    }

    public long getTotalTmpItems() {
        return rawPagesProcessed.get();
    }

    public float getTmpRate() {
        return getTotalTmpItems() / (runtime() / 1000);
    }

    public long runtime() {
        return System.currentTimeMillis() - tmpStartedAt;
    }

    public StringBuilder sprint() {
        StringBuilder s = new StringBuilder("<< Stats >>\n");
        s.append(String.format("Runtime: %ds TotalTmpItems: %d, TmpRate: %f\n",
                runtime() / 1000,
                getTotalTmpItems(),
                getTmpRate()
        ));
        s.append(String.format("Temp DB Items: Articles(%d) Redirects(%d)\n",
                tmpArticlesProcessed.get(),
                tmpRedirectsProcessed.get()));
        s.append(String.format("Queue Levels: Articles(%d) Redirects(%d)\n",
                processor.articlesQueue.size(),
                processor.redirectsQueue.size()));
        return s;
    }

    public void startProcessorStatsPrinting() {
        TimerTask tt = new TimerTask() {
            @Override
            public void run() {
                System.out.println(sprint());
            }
        };
        processorStatsPrintTimer.scheduleAtFixedRate(tt,1000, 1000);
    }

    public void stopProcessorStatsPrinting() {
        processorStatsPrintTimer.cancel();
    }
}
