package swift.utils;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

/**
 * Durable log using Kryo serialization and disk as a storage.
 * <p>
 * All written objects must by Kryolizable.
 * 
 * @author mzawirski
 */
public class KryoDiskLog implements TransactionsLog {
    private final Kryo kryo;
    private final Output output;

    /**
     * @param fileName
     *            file where objects are written
     * @throws FileNotFoundException
     */
    public KryoDiskLog(final String fileName) throws FileNotFoundException {
        kryo = new Kryo();
        output = new Output(new FileOutputStream(fileName));
    }

    @Override
    public synchronized void writeEntry(final long transactionId, final Object object) {
        kryo.writeObject(output, transactionId);
        kryo.writeObject(output, object);
    }

    @Override
    public synchronized void flush() {
        output.flush();
    }

    @Override
    public synchronized void close() {
        output.close();
    }
}
