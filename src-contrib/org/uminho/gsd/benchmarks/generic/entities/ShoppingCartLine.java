/*
 * *********************************************************************
 * Copyright (c) 2010 Pedro Gomes and Universidade do Minho.
 * All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ********************************************************************
 */

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.uminho.gsd.benchmarks.generic.entities;

import org.uminho.gsd.benchmarks.interfaces.Entity;

import java.util.TreeMap;

public class ShoppingCartLine implements Entity {

	int ShoppingCartID;
	int book;
	int cart;
	int qty;

	float SCL_COST;// The cost of the item in the CART
	float SCL_SRP;// The list price for the item in the CART
	String SCL_TITLE;// The title of the item in the CART
	String SCL_BACKING;// The backing of the item in the CART

	public ShoppingCartLine(int ShoppingCartID, int book, int cart, int qty) {
		this.ShoppingCartID = ShoppingCartID;
		this.book = book;
		this.cart = cart;
		this.qty = qty;
	}

	public int getBook() {
		return book;
	}

	public void setBook(int book) {
		this.book = book;
	}

	public int getCart() {
		return cart;
	}

	public void setCart(int cart) {
		this.cart = cart;
	}

	public int getShoppingCartID() {
		return ShoppingCartID;
	}

	public void setShoppingCartID(int shoppingCartID) {
		ShoppingCartID = shoppingCartID;
	}

	public int getQty() {
		return qty;
	}

	public void setQty(int qty) {
		this.qty = qty;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("QTY", qty);
		values.put("KEY_BOOK", book);
		values.put("KEY_SHOPPING_CART", cart);

		return values;
	}

	public String getKeyName() {
		return "KEY_SHOPPING_CART";
	}

	@Override
	public Object copy() {
		ShoppingCartLine scl = new ShoppingCartLine(ShoppingCartID, book,
				ShoppingCartID, qty);
		scl.SCL_BACKING = SCL_BACKING;
		scl.SCL_COST = SCL_COST;
		scl.SCL_SRP = SCL_SRP;
		scl.SCL_TITLE = SCL_TITLE;
		return scl;
	}

}