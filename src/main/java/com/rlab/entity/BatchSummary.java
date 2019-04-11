package com.rlab.entity;

import java.io.Serializable;

public class BatchSummary implements Serializable {
	private String batchId;
	private String batchSize;
	private float p0;
	private float p1;
	private int n0;
	private int n1;
	private int noOffers;
	private int giftOffers;
	private int discountOffers;
	
	public String getBatchId() {
		return batchId;
	}
	public void setBatchId(String batchId) {
		this.batchId = batchId;
	}
	public String getBatchSize() {
		return batchSize;
	}
	public void setBatchSize(String batchSize) {
		this.batchSize = batchSize;
	}
	public float getP0() {
		return p0;
	}
	public void setP0(float p0) {
		this.p0 = p0;
	}
	public float getP1() {
		return p1;
	}
	public void setP1(float p1) {
		this.p1 = p1;
	}
	public int getN0() {
		return n0;
	}
	public void setN0(int n0) {
		this.n0 = n0;
	}
	public int getN1() {
		return n1;
	}
	public void setN1(int n1) {
		this.n1 = n1;
	}
	public int getNoOffers() {
		return noOffers;
	}
	public void setNoOffers(int noOffers) {
		this.noOffers = noOffers;
	}
	public int getGiftOffers() {
		return giftOffers;
	}
	public void setGiftOffers(int giftOffers) {
		this.giftOffers = giftOffers;
	}
	public int getDiscountOffers() {
		return discountOffers;
	}
	public void setDiscountOffers(int discountOffers) {
		this.discountOffers = discountOffers;
	}
	
	
	
}
