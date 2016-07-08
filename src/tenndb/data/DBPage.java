package tenndb.data;


public class DBPage {
	
//	public static final int MAX_BLOCK_SIZE = 10; 
	
	public static final int INT_SIZE = Integer.SIZE / Byte.SIZE;

	//len
//	public static final int BLOCK_HEAD_SIZE = INT_SIZE;
	//data
//	public static final int BLOCK_BODY_SIZE = INT_SIZE * 24;

//	public static final int BLOCK_SIZE = BLOCK_HEAD_SIZE + BLOCK_BODY_SIZE;

	//(num,key,data) x86: (1 + 4 + 40)*20=900
	
	  //pageid + size
	public static final int HEAD_SIZE = 0;//INT_SIZE + INT_SIZE ;
	public static final int BLOCK_SIZE = INT_SIZE;
	
	public static final int PAGE_SIZE = 10240;
									//HEAD_SIZE +  
									//(key + offset)*BALANCE_SIZE 48
									//   BLOCK_SIZE * MAX_BLOCK_SIZE;

	
	
	
	protected static final int NEW_PAGES_SIZE = 100;

	protected static final int PAGE_INDEX_SIZE = INT_SIZE + INT_SIZE;
	
}
