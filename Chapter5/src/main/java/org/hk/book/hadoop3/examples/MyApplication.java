package org.hk.book.hadoop3.examples;

public class MyApplication {

	  public MyApplication() {
	    System.out.println("MyApplication !");
	  }


	  public void printHelloWorld() {
		  System.out.println("Hello World with YARN");
	  }

	  public static void main(String[] args) {
		  MyApplication myApp = new MyApplication();
		  myApp.printHelloWorld();
	  }
}
