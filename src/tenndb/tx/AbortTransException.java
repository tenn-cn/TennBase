package tenndb.tx;

public class AbortTransException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4369948384114813894L;

	public AbortTransException(String message) {
		super(message);
	}	
}
