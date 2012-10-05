package loria.swift.crdt.logoot;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import loria.swift.application.filesystem.FileContent;
import swift.clocks.CausalityClock;
import swift.clocks.TripleTimestamp;
import swift.crdt.BaseCRDTTxnLocal;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.Copyable;
import swift.crdt.interfaces.TxnHandle;

public class LogootTxnLocal<V extends Copyable> extends BaseCRDTTxnLocal<LogootVersionned<V>> implements FileContent {
    private final LogootDocument<V> doc;
    private final Random ran = new Random();
    private static final long BOUND = 1000000000l;
    private final BigInteger boundBI;
    private static final int NBBIT = 64;
    private final long max;
    private final BigInteger base;
        
    public LogootTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, LogootVersionned<V> creationState,
            LogootDocument doc) {
        super(id, txn, clock, creationState);
        this.doc = doc;
        this.boundBI = BigInteger.valueOf(BOUND);
        if (NBBIT == 64) {
            this.max = Long.MAX_VALUE;
        } else {
            this.max = (long) Math.pow(2, NBBIT) - 1;
        }
        this.base = BigInteger.valueOf(2).pow(NBBIT);
    }

    public long nextLong(long l) {
        long x = ran.nextLong() % l;
        if (x < 0) {
            x += l;
        }
        return x;
    }
    
    /**
     * Generates N identifier between P and Q. Uses boundary strategy.
     */
    ArrayList<LogootIdentifier> generateLineIdentifiers(LogootIdentifier P, LogootIdentifier Q, int n) {
        int index = 0, tMin = Math.min(P.length(), Q.length());
        
        while ((index < tMin && P.getComponentAt(index).equals(Q.getComponentAt(index))   
                || (P.length() <= index && Q.length() > index && Q.getDigitAt(index) == 0))) {
            index++;
        }         
        
        long interval, d = Q.getDigitAt(index) - P.getDigitAt(index) - 1;
        if (d >= n) {
            interval = Math.min(d/n, BOUND); 
        } else {
            BigInteger diff = d == -1 ? BigInteger.ZERO : BigInteger.valueOf(d),
                    N = BigInteger.valueOf(n);
            while (diff.compareTo(N) < 0) {
                index++;
                diff = diff.multiply(base).
                        add(BigInteger.valueOf(max - P.getDigitAt(index)).
                        add(BigInteger.valueOf(Q.getDigitAt(index))));
            }           
            interval = diff.divide(N).min(boundBI).longValue();
        }
        
        ArrayList<LogootIdentifier> patch = new ArrayList<LogootIdentifier>();        
        List<Long> digits = P.digits(index);
        for (int i = 0; i < n; i++) {
            LogootStrategy.plus(digits, nextLong(interval) + 1, base, max);
            P = LogootStrategy.constructIdentifier(digits, P, Q, nextTimestamp());
            patch.add(P);
        }  
        return patch;
    }
    
    /**
     * Inserts a list of line in local document.
     */
    public void insert(List<String> content, int position) {
        int N = content.size();
        ArrayList<LogootIdentifier> patch = generateLineIdentifiers(doc.idTable.get(position),
                doc.idTable.get(position + 1), N);
        ArrayList<V> lc = new ArrayList<V>(N);
        
        for (int cmpt = 0; cmpt < patch.size(); cmpt++) {
            registerLocalOperation(new LogootInsert(patch.get(cmpt), content.get(cmpt)));
        }
        doc.insert(position, patch, lc);
    }
    
    /**
     * Deletes a range of line in local document.
     */
    public void delete(int position, int offset) {
        for (int cmpt = 0; cmpt < offset; cmpt++) {
            registerLocalOperation(new LogootDelete(doc.idTable.get(position + cmpt)));
        }
        doc.delete(position, offset);
    }
    
    @Override
    public List<V> getValue() {
        return doc.document;
    }

    @Override
    public Object executeQuery(CRDTQuery<LogootVersionned<V>> query) {
        return query.executeAt(this);
    }

    @Override
    public void update(String newValue) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String getText() {
        StringBuilder sb = new StringBuilder();
        for (V e : doc.document) {
            sb.append(e.toString());
        }
        return sb.toString();
    }
}
