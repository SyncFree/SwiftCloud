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

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import sys.utils.Threading;

/**
 * A task with deadline that can be retried a number of times in case of
 * (transient) failure.
 * 
 * @author mzawirski
 */
public abstract class CallableWithDeadline<V> implements Callable<V> {

	private long startTime = sys.Sys.Sys.timeMillis(), rtt = -1L;
    private final long deadlineTime;
    
    final private V defaultResult;
    private AtomicReference<V> result = new AtomicReference<V>();
    
    /**
     * Sets the result of the call, holding the first non-null result.
     */
    protected void setResult( V res ) {
    	if( result.get() == null )
    		result.set( res ) ;
    	
    	if( rtt < 0 ) {
    		rtt = sys.Sys.Sys.timeMillis() - startTime;
    	}
    	Threading.synchronizedNotifyAllOn(this);
    }
    
    /*
     * Return the current result for the call. Null if there is no result.
     */
    protected V getResult() {
    	V res = result.get() ;
    	return res == null ? defaultResult : res ;
    }
    
    /**
     * Creates callable task without deadline.
     */
    public CallableWithDeadline(V defaultResult) {
    	this.defaultResult = defaultResult;
        this.deadlineTime = Long.MAX_VALUE;
    }

//    /**
//     * Creates callable task with provided deadline in milliseconds.
//     */
//    public CallableWithDeadline(long deadlineMillis) {
//        this.deadlineTime = System.currentTimeMillis() + deadlineMillis;
//        this.defaultResult = null;
//        Thread.dumpStack();
//    }

    /**
     * Creates callable task with provided deadline in milliseconds.
     */
    public CallableWithDeadline(V defaultResult, long deadlineMillis) {
    	this.defaultResult = defaultResult;
        this.deadlineTime = System.currentTimeMillis() + deadlineMillis;
    }

    int tries = 0 ;
    /**
     * Executes (or retries) a task. Throws an exception if task needs to be
     * retried.
     * 
     * @see Callable#call()
     */
    @Override
    public V call() throws Exception {
    	V res = result.get();
    	if( res == null )
    		res = callOrFailWithNull() ;

    	if (res == null && result.get() == null) {
            throw new Exception("Task needs a retry");
        }
        return getResult();
    }

    /**
     * Executes a task.
     * 
     * @return return value, or null if task should be retried.
     */
    protected abstract V callOrFailWithNull();

    public int getDeadlineLeft() {
        // This is (int) only for RPC lib convenience.
        return (int) Math.min(Math.max(deadlineTime - System.currentTimeMillis(), 0), Integer.MAX_VALUE);
    }

    public long getDeadlineTime() {
        return deadlineTime;
    }
}
