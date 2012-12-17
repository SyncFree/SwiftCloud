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
package sys.net.impl.providers;

import static sys.net.impl.NetworkingConstants.NIO_CORE_POOL_THREADS;
import static sys.net.impl.NetworkingConstants.NIO_EXEC_QUEUE_SIZE;
import static sys.net.impl.NetworkingConstants.NIO_MAX_IDLE_THREAD_IMEOUT;
import static sys.net.impl.NetworkingConstants.NIO_MAX_POOL_THREADS;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SingleQueueExecutor {
    final BlockingQueue<Runnable> holdQueue = new ArrayBlockingQueue<Runnable>(NIO_EXEC_QUEUE_SIZE);
    final ThreadPoolExecutor threadPool = new ThreadPoolExecutor(NIO_CORE_POOL_THREADS, NIO_MAX_POOL_THREADS,
            NIO_MAX_IDLE_THREAD_IMEOUT, TimeUnit.SECONDS, holdQueue);

    public SingleQueueExecutor() {
        threadPool.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
    }

    public void execute(final Object queueSelector, final Runnable task) {
        threadPool.execute(task);
    }
}
