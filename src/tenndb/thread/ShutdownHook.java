package tenndb.thread;

import tenndb.base.Catalog;

public class ShutdownHook extends Thread {

	protected Catalog catalog = null;
	
	public ShutdownHook(Catalog catalog) {
		super();
		this.catalog = catalog;
	}

	public void run(){
		System.out.println("+ShutdownHook");
		this.catalog.flush();
		System.out.println("-ShutdownHook");
	}
}
