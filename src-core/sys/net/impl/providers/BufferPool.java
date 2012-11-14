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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static sys.net.impl.NetworkingConstants.*;

public class BufferPool<V> {

	final BlockingQueue<V> bufferPool;

	public BufferPool() {
		this(KRYOBUFFERPOOL_SIZE);
	}

	public BufferPool(int size) {
		bufferPool = new ArrayBlockingQueue<V>(size);
	}

	public V poll() {
		try {
			return bufferPool.poll( KRYOBUFFERPOOL_TIMEOUT, TimeUnit.MILLISECONDS ) ;
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}

	public V take() {
		try {
			return bufferPool.take();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public void offer(V buffer) {
		bufferPool.offer(buffer);
	}

	public int size() {
		return bufferPool.size();
	}

	public int remainingCapacity() {
		return bufferPool.remainingCapacity();
	}
	
	public void release() {
		bufferPool.clear();
	}
}
