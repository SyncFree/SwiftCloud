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
package sys.dht.catadupa;

import static sys.Sys.Sys;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * 
 * @author smd
 * 
 * @param <T>
 */
public class RandomList<T> extends ArrayList<T> {

	public RandomList() {
	}

	public RandomList(Collection<? extends T> c) {
		super(c);
	}

	public RandomList(Iterator<? extends T> it) {
		for (; it.hasNext();)
			add(it.next());
	}

	public T randomElement() {
		return isEmpty() ? null : get(Sys.rg.nextInt(super.size()));
	}

	public T removeRandomElement() {
		return isEmpty() ? null : remove(Sys.rg.nextInt(super.size()));
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
}
