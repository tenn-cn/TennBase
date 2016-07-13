package tenndb.index;


import java.nio.ByteBuffer;

import tenndb.bstar.RandomInsertBStarTree;
import tenndb.bstar.BTreeNode;
import tenndb.bstar.IdxBlock;


public class IndexPage {

	public static final int INT_SIZE  = Integer.SIZE / Byte.SIZE;
	public static final int BYTE_SIZE = Byte.SIZE / Byte.SIZE;
	public static final int INDEX_HEAD_SIZE = INT_SIZE;
	
	public static final int PAGE_HEAD_SIZE  = BYTE_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + INT_SIZE ;
	public static final int PAGE_BLOCK_SIZE = INT_SIZE + INT_SIZE + INT_SIZE + INT_SIZE + BYTE_SIZE ;
	public static final int PAGE_BODY_SIZE  = PAGE_BLOCK_SIZE * (RandomInsertBStarTree.BALANCE_SIZE + 2);
	//(num,key,data) x86: (1 + 4 + 40)*20=900
									//type + pageid + parentid + priorid + nextid + size
	public static final int PAGE_SIZE = PAGE_HEAD_SIZE + PAGE_BODY_SIZE;
									//(key + pageid + blockid + tag)*BALANCE_SIZE 48
										
	
	protected long pos          = 0;
	protected BTreeNode node    = null;
	protected ByteBuffer buffer = null;
	
	public IndexPage(BTreeNode node, ByteBuffer buffer) {
		super();
		this.node = node;
		this.buffer = buffer;
	}
	
	public ByteBuffer getBuffer() {
		return buffer;
	}

	public IdxBlock getBlock(int index){
		return this.node.getBlock(index);
	}
	
	public int size(){
		return this.node.size();
	}

	public long getPos() {
		return pos;
	}

	public void setPos(long pos) {
		this.pos = pos;
	}

	public void flush(BTreeNode rootNode){
		this.node.toByteBuffer(this.buffer, rootNode);
	}
	
	public int pageID(){
		return this.node.getPageID();
	}
}
