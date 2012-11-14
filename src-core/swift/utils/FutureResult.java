/*****************************************************************************
 * Copyright 2011-2012 INRIA
 * Copyright 2011-2012 Universidade Nova de Lisboa
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *****************************************************************************/
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
