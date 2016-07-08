package tenndb.test;

public class TestRun {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

        	Runtime.getRuntime().addShutdownHook(new Thread() {
                 public void run() { System.out.println("wwwwww"); }
            });

	}

}
