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

import java.sql.Date;
import java.util.TreeMap;

/**
 * I_ID I_TITLE I_A_ID I_PUB_DATE X I_PUBLISHER I_SUBJECT I_DESC I_RELATED[1 -5]
 * I_THUMBNAIL X I_IMAGE X I_SRP X I_COST I_AVAIL I_STOCK I_ISBN I_PAGE
 * I_BACKING I_DIMENSION
 */
public class Item implements Entity {

	int i_id; // ID
	String I_TITLE;
	Date pubDate;
	int I_AUTHOR;
	String I_PUBLISHER;
	String I_DESC;
	String I_SUBJECT;
	String thumbnail;
	String image;
	double I_COST;
	int I_STOCK;
	String isbn;// international id
	double srp;// Suggested Retail Price
	int I_RELATED1;
	int I_RELATED2;
	int I_RELATED3;
	int I_RELATED4;
	int I_RELATED5;
	int I_PAGE;
	Date avail; // Data when available
	String I_BACKING;
	String dimensions;

	public Item(int i_id, String I_TITLE, Date pubDate, String I_PUBLISHER,
			String I_DESC, String I_SUBJECT, String thumbnail, String image,
			double I_COST, int I_STOCK, String isbn, double srp,
			int I_RELATED1, int I_RELATED2, int I_RELATED3, int I_RELATED4,
			int I_RELATED5, int I_PAGE, Date avail, String I_BACKING,
			String dimensions, int author) {
		this.i_id = i_id;
		this.I_TITLE = I_TITLE;
		this.pubDate = pubDate;
		this.I_AUTHOR = author;
		this.I_PUBLISHER = I_PUBLISHER;
		this.I_DESC = I_DESC;
		this.I_SUBJECT = I_SUBJECT;
		this.thumbnail = thumbnail;
		this.image = image;
		this.I_COST = I_COST;
		this.I_STOCK = I_STOCK;
		this.isbn = isbn;
		this.srp = srp;
		this.I_RELATED1 = I_RELATED1;
		this.I_RELATED2 = I_RELATED2;
		this.I_RELATED3 = I_RELATED3;
		this.I_RELATED4 = I_RELATED4;
		this.I_RELATED5 = I_RELATED5;
		this.I_PAGE = I_PAGE;
		this.avail = avail;
		this.I_BACKING = I_BACKING;
		this.dimensions = dimensions;

	}

	public int getI_AUTHOR() {
		return I_AUTHOR;
	}

	public void setI_AUTHOR(int i_AUTHOR) {
		I_AUTHOR = i_AUTHOR;
	}

	public String getI_BACKING() {
		return I_BACKING;
	}

	public void setI_BACKING(String I_BACKING) {
		this.I_BACKING = I_BACKING;
	}

	public double getI_COST() {
		return I_COST;
	}

	public void setI_COST(double i_COST) {
		I_COST = i_COST;
	}

	public String getI_DESC() {
		return I_DESC;
	}

	public void setI_DESC(String I_DESC) {
		this.I_DESC = I_DESC;
	}

	public int getI_PAGE() {
		return I_PAGE;
	}

	public void setI_PAGE(int I_PAGE) {
		this.I_PAGE = I_PAGE;
	}

	public String getI_PUBLISHER() {
		return I_PUBLISHER;
	}

	public void setI_PUBLISHER(String I_PUBLISHER) {
		this.I_PUBLISHER = I_PUBLISHER;
	}

	public int getI_STOCK() {
		return I_STOCK;
	}

	public void setI_STOCK(int i_STOCK) {
		I_STOCK = i_STOCK;
	}

	public String getI_SUBJECT() {
		return I_SUBJECT;
	}

	public void setI_SUBJECT(String I_SUBJECT) {
		this.I_SUBJECT = I_SUBJECT;
	}

	public String getI_TITLE() {
		return I_TITLE;
	}

	public void setI_TITLE(String I_TITLE) {
		this.I_TITLE = I_TITLE;
	}

	public Date getAvail() {
		return avail;
	}

	public void setAvail(Date avail) {
		this.avail = avail;
	}

	public String getDimensions() {
		return dimensions;
	}

	public void setDimensions(String dimensions) {
		this.dimensions = dimensions;
	}

	public int getI_id() {
		return i_id;
	}

	public void setI_id(int i_id) {
		this.i_id = i_id;
	}

	public String getImage() {
		return image;
	}

	public void setImage(String image) {
		this.image = image;
	}

	public String getIsbn() {
		return isbn;
	}

	public void setIsbn(String isbn) {
		this.isbn = isbn;
	}

	public Date getPubDate() {
		return pubDate;
	}

	public void setPubDate(Date pubDate) {
		this.pubDate = pubDate;
	}

	public double getSrp() {
		return srp;
	}

	public void setSrp(double srp) {
		this.srp = srp;
	}

	public String getThumbnail() {
		return thumbnail;
	}

	public void setThumbnail(String thumbnail) {
		this.thumbnail = thumbnail;
	}

	public TreeMap<String, Object> getValuesToInsert() {
		//
		//
		// I_ID
		// I_TITLE
		// I_A_ID
		// I_PUB_DATE
		// I_PUBLISHER
		// I_SUBJECT
		// I_DESC
		// I_RELATED[1-5]
		// I_THUMBNAIL
		// I_IMAGE
		// I_SRP
		// I_COST
		// I_AVAIL
		// I_STOCK
		// I_ISBN
		// I_PAGE
		// I_BACKING
		// I_DIMENSION

		TreeMap<String, Object> values = new TreeMap<String, Object>();

		values.put("I_TITLE", I_TITLE);
		values.put("I_A_ID", I_AUTHOR);
		values.put("I_PUB_DATE", pubDate);
		values.put("I_PUBLISHER", I_PUBLISHER);
		values.put("I_SUBJECT", I_SUBJECT);
		values.put("I_DESC", I_DESC);
		values.put("I_RELATED1", I_RELATED1);
		values.put("I_RELATED2", I_RELATED2);
		values.put("I_RELATED3", I_RELATED3);
		values.put("I_RELATED4", I_RELATED4);
		values.put("I_RELATED5", I_RELATED5);
		values.put("I_THUMBNAIL", thumbnail);
		values.put("I_IMAGE", image);
		values.put("I_SRP", srp);
		values.put("I_COST", I_COST);
		values.put("I_AVAIL", avail);
		values.put("I_STOCK", I_STOCK);
		values.put("I_ISBN", isbn);
		values.put("I_PAGE", I_PAGE);
		values.put("I_BACKING", I_BACKING);
		values.put("I_DIMENSION", dimensions);

		return values;
	}

	public String getKeyName() {
		return "I_ID";
	}

	@Override
	public Object copy() {
		return new Item(i_id, I_TITLE, pubDate, I_PUBLISHER, I_DESC, I_SUBJECT,
				thumbnail, image, I_COST, I_STOCK, isbn, srp, I_RELATED1,
				I_RELATED2, I_RELATED3, I_RELATED4, I_RELATED5, I_PAGE, avail,
				I_BACKING, dimensions, I_AUTHOR);
	}

}