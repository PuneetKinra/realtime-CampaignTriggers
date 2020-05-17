package com.yesbank.camptriggers;

public class Test {

	public static void main(String[] args) {
		try {
			for(int i=1;i<=4;i++) {
				System.out.println(i);
				if(i%2==0) {
					throw new Exception("even");
				}
				

			}
		}catch(Exception e) {
			System.out.println(e);
		}
	}
}
