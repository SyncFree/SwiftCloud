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
package pt.citi.cs.crdt.benchmarks.tpcw.synchronization;

public enum MessageSize {
	OP_ITEM_INFO(1726), OP_HOME(7172), OP_SEARCH(2004), OP_LOGIN(2056), OP_SHOPPING_CART(
			2056), OP_BUY_REQUEST(3690), OP_BUY_CONFIRM(1530), OP_REGISTER(2769), OP_ADMIN_CHANGE(
			1349), OP_BEST_SELLERS(6640), OP_ORDER_INQUIRY(953), OP_NEW_PRODUCTS(8095);

	private final int size;

	MessageSize(int size) {
		this.size = size;
	}

	int size() {
		return size;
	}

}
