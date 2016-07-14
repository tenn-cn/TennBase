package tenndb.data;

public class Filed {

	public static final byte BYTE  = 1;
	public static final byte WORD  = 2;
	public static final byte DWORD = 4;
	public static final byte LWORD = 8;
	
	protected byte   type;
	protected String name;
	protected String value;
	
	public Filed(String name, String value) {
		super();
		this.name = name;
		this.value = value;
	}
	public String getName() {
		return name;
	}
	public String getValue() {
		return value;
	}
}
