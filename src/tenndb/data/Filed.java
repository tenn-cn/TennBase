package tenndb.data;

public class Filed {

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
