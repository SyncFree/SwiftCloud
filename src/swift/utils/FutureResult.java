package swift.utils;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class FutureResult<V>
    implements Future<V> {
    private V result;
    private boolean hasResult;
    
    public FutureResult() {
        hasResult = false;
    }
    
    public synchronized void setResult( V res) {
            this.result = res;
            hasResult = true;
            notifyAll();
    }

    @Override
    public boolean cancel(boolean arg0) {
        return false;
    }

    @Override
    public synchronized V get() throws InterruptedException, ExecutionException {
            while( ! hasResult)
                wait();
            return result;
    }

    @Override
    public synchronized V get(long arg0, TimeUnit arg1) throws InterruptedException, ExecutionException, TimeoutException {
            while( ! hasResult)
                wait( arg1.toMillis(arg0));
            return result;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public synchronized boolean isDone() {
        return hasResult;
    }

}
