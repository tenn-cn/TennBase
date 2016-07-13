package tenndb.base;

import java.util.concurrent.atomic.AtomicReference;

import tenndb.thread.ShutdownHook;

public class TennBase {

    private static AtomicReference<TennBase> _instance = new AtomicReference<TennBase>(new TennBase());
    
    protected final Catalog catalog;
    protected final ShutdownHook shutdown;
    
	private TennBase(){
		String root = "J:\\tennbase";
		this.catalog = new Catalog(root);
		this.catalog.recover();
		this.shutdown = new ShutdownHook(this.catalog);
    	Runtime.getRuntime().addShutdownHook(this.shutdown);    	
	}

	public static Catalog getCatalog(){
		return _instance.get().catalog;
	} 
}
