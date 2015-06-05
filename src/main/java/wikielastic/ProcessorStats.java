package wikielastic;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by andrewcholakian on 6/4/15.
 */
public class ProcessorStats {
    public volatile Long tmpStartedAt = null;
    public volatile Long endedAt = null;
    public final AtomicLong rawPagesProcessed = new AtomicLong(0);
    public final AtomicLong tmpArticlesProcessed = new AtomicLong(0);
    public final AtomicLong tmpRedirectsProcessed = new AtomicLong(0);

    public long getTotalTmpItems() {
        return rawPagesProcessed.get();
    }

    public float getTmpRate() {
        return getTotalTmpItems() / (runtime() / 1000);
    }

    public long runtime() {
        return System.currentTimeMillis() - tmpStartedAt;
    }
}
