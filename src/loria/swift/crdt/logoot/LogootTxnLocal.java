package loria.swift.crdt.logoot;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import loria.swift.application.filesystem.mapper.FileContent;
import org.eclipse.jgit.diff.DiffAlgorithm;
import org.eclipse.jgit.diff.Edit;
import org.eclipse.jgit.diff.EditList;
import org.eclipse.jgit.diff.RawText;
import org.eclipse.jgit.diff.RawTextComparator;
import swift.application.filesystem.Blob;
import swift.clocks.CausalityClock;
import swift.crdt.BaseCRDTTxnLocal;
import swift.crdt.CRDTIdentifier;
import swift.crdt.interfaces.CRDTQuery;
import swift.crdt.interfaces.TxnGetterSetter;
import swift.crdt.interfaces.TxnHandle;

/**
 * Logoot transaction. Is also a FileContent.
 * @author urso
 */
public class LogootTxnLocal extends BaseCRDTTxnLocal<LogootVersioned> implements  TxnGetterSetter<Blob>,FileContent {
    private static final long BOUND = 1000000000l;
    private static final int NBBIT = 64;
    final static long max = Long.MAX_VALUE;

    private static final BigInteger boundBI = BigInteger.valueOf(BOUND);
    private static final BigInteger base = BigInteger.valueOf(2).pow(NBBIT);

    private final LogootDocument<String> doc;
    private final Random ran = new Random();
    
    public static final DiffAlgorithm diffAlgorithm = DiffAlgorithm.getAlgorithm(DiffAlgorithm.SupportedAlgorithm.MYERS);
    
    public LogootTxnLocal(CRDTIdentifier id, TxnHandle txn, CausalityClock clock, LogootVersioned creationState,
            LogootDocument doc) {
        super(id, txn, clock, creationState);
        this.doc = doc;
    }


    LogootDocument<String> getDoc() {
        return doc;
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
    private void insert(RawText content, int position, int length) {
        ArrayList<LogootIdentifier> patch = generateLineIdentifiers(doc.idTable.get(position),
                doc.idTable.get(position + 1), length);
        ArrayList<String> lc = new ArrayList<String>(length);
        
        for (int cmpt = 0; cmpt < length; cmpt++) {
            String v = content.getString(position+cmpt);
            registerLocalOperation(new LogootInsert(patch.get(cmpt), v));
            lc.add(v);
        }
        doc.insert(position, patch, lc);
    }
    
    /**
     * Deletes a range of line in local document.
     */
    private void delete(int position, int offset) {
        for (int cmpt = 1; cmpt <= offset; cmpt++) {
            registerLocalOperation(new LogootDelete(doc.idTable.get(position + cmpt), nextTimestamp()));
        }
        doc.delete(position, offset);
    }
    
    
    public List<String> getValues() {
        return doc.document;
    }

    @Override
    public Object executeQuery(CRDTQuery<LogootVersioned> query) {
        return query.executeAt(this);
    }

    /**
     * Makes a diff to realize a content update.
     */
    @Override
    public void set(String newValue) {
        final RawText a = new RawText(getText().getBytes());
        final RawText b = new RawText(newValue.getBytes());
        final EditList editList = diffAlgorithm.diff(RawTextComparator.DEFAULT, a, b);
        for (Edit e : editList) {
            if (e.getType() != Edit.Type.INSERT) { // del or repl
                delete(e.getBeginA(), e.getLengthA());
            } 
            if (e.getType() != Edit.Type.DELETE) { // ins or repl
                insert(b, e.getBeginB(), e.getLengthB());
            }
        }
    }

    @Override
    public String getText() {
        return doc.toString();
    }

    @Override
    public void set(Blob v) {
        this.set(new String (v.get()));
    }

    @Override
    public Blob getValue() {
        return new Blob(this.getText().getBytes());
    }
}
