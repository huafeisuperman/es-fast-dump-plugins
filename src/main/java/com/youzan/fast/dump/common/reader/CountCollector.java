package com.youzan.fast.dump.common.reader;

import com.youzan.fast.dump.common.BaseLogger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.ElasticsearchException;

import java.io.IOException;

/**
 * Description:
 *
 * @author: huafei
 * @date: 2020.07.30
 */
public class CountCollector extends SimpleCollector implements BaseLogger {
    private int totalHits;
    private FixedBitSet bitSet;
    private int docBase;

    public CountCollector(FixedBitSet bitSet) {
        this.bitSet = bitSet;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) throws IOException {
        super.doSetNextReader(context);
        docBase = context.docBase;

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
        if (totalHits > 1000000000) {
            throw new ElasticsearchException("query match count can not gt 10亿");
        }
        bitSet.set(doc + docBase);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE_NO_SCORES;
    }
}
