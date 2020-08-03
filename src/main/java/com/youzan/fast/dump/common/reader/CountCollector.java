package com.youzan.fast.dump.common.reader;

import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.30
 */
public class CountCollector extends SimpleCollector {
    private int totalHits;
    private FixedBitSet bitSet;

    public CountCollector(FixedBitSet bitSet) {
        this.bitSet = bitSet;
    }

    /**
     * Returns how many hits matched the search.
     */
    public int getTotalHits() {
        return totalHits;
    }

    @Override
    public void collect(int doc) {
        totalHits++;
        bitSet.set(doc);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }
}
