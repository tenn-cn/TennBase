package tenndb.index;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import tenndb.bstar.BStarTree;
import tenndb.bstar.BTreeNode;
import tenndb.bstar.IdxBlock;
import tenndb.common.ByteUtil;
import tenndb.common.FileDeco;
import tenndb.common.FileMgr;
import tenndb.data.ByteBufferMgr;
import tenndb.tx.TransMgr;

public class IndexMgr {

	protected IBTree tree = null;
	
	protected String dbName = null;
	
	protected FileMgr fileMgr = null;
	
	protected ByteBufferMgr bufferMgr = null;
	
	protected static final String PREFIX_TEMP_INDEX = "temp_index_";
	
	protected static final String PREFIX_SNAPSHOT_INDEX = "snapshot_index_";
	
	protected static final String PREFIX_INDEX = "index_";
	
	protected static final String PREFIX_BAK_INDEX = "bak_index_";
	
	protected int lastNodeID = -1;
	
	protected AtomicInteger counter = new AtomicInteger(1);
	
	protected Map<Integer, IndexPage> pageMap = null;
	
	protected List<IdxBlock> blockList = null;
	
	protected Set<Integer> newPageList = null;
	
	protected Set<Integer> dirtyPageList = null;
	
	protected TransMgr transMgr = null;

	

	
	public IndexMgr(String dbName, FileMgr fileMgr, TransMgr transMgr) {
		super();
		this.dbName        = dbName;
		this.tree          = null; 
		this.fileMgr       = fileMgr;
		this.transMgr      = transMgr;
		this.bufferMgr     = new ByteBufferMgr(IndexPage.PAGE_SIZE);
		this.pageMap       = new Hashtable<Integer, IndexPage>();
		this.newPageList   = new HashSet<Integer>();
		this.dirtyPageList = new HashSet<Integer>();
		this.blockList     = new ArrayList<IdxBlock>();
	}
	
	public boolean flushBlock(IdxBlock blk){
		boolean b = false;
		
		if(null != blk){			
			IndexPage page = this.pageMap.get(blk.getIdxPageID());
			if(null != page){
				long pos = page.pos + IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * blk.getIdxOffset();
				ByteBuffer buff = ByteBuffer.allocate(IndexPage.PAGE_BLOCK_SIZE);
				byte[] array = buff.array();
//				System.out.println("flushBlock " + pos);
				try {
					this.fileMgr.writeBuffer(PREFIX_SNAPSHOT_INDEX + this.dbName, array, pos);
					b = true;
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return b;
	}
	
	public IndexPage addIndexPage(BTreeNode node, ByteBuffer buffer){
		IndexPage page = null;
		if(null != node && null != buffer){
			page = new IndexPage(node, buffer);
	//		System.out.println("pinIndexPage " + page.pageID());
			this.pageMap.put(page.pageID(), page);
		}
		return page;
	}
	
	public IndexPage appendNewIndexPage(BTreeNode newNode, BTreeNode oldNode){
		IndexPage newPage = null;
		if(null != newNode){
			int nodeID = this.nextNodeID();
//			System.out.println("appendNewIndexPage " + nodeID);
			newNode.setPageID(nodeID);
			ByteBuffer buffer = this.bufferMgr.pinBuffer();
			newPage = new IndexPage(newNode, buffer);
			newNode.setRefPage(newPage);
			this.pageMap.put(newPage.pageID(), newPage);
			this.newPageList.add(newPage.pageID());
		}
		
		if(null != oldNode){
			IndexPage oldPage = oldNode.getRefPage(); 
//			System.out.println("dirtyPageList.1 " + oldNode.getPageID());
			if(null != oldPage){
//				System.out.println("dirtyPageList.2 " + oldPage.pos + ", " + oldPage.pageID());
				
				this.dirtyPageList.add(oldPage.pageID());
			}
		}
		return newPage;
	}
	
	public boolean flushNewPages(){
		boolean b = false;
		if(this.newPageList.size() > 0){
			byte[] array = ByteUtil.intToByte4_big(this.counter.get());
			try {
				this.fileMgr.writeBuffer(PREFIX_SNAPSHOT_INDEX + this.dbName, array, 0);
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			for(Integer pid : this.newPageList){
				IndexPage page = this.pageMap.get(pid);
				if(null != page){
					page.flush(this.tree.getRoot());
					long pos = this.fileMgr.append(PREFIX_SNAPSHOT_INDEX + this.dbName, page.buffer);
					page.setPos(pos);
				}

//				System.out.println("new " + page.pageID() + ", " + page.pos + ", " + page.node.isLeaf());
			}
			
//			this.tree.printTreeNext();
//			System.out.println("+++++++++++++++++++++++++++++++++++++++++++");
			this.newPageList.clear();
			b = true;
		}
		
		if(this.dirtyPageList.size() > 0){
			for(Integer pid : this.dirtyPageList){
				
				IndexPage page = this.pageMap.get(pid);
				if(null != page){
					page.flush(this.tree.getRoot());
					byte[] array = page.buffer.array();
					try {
						this.fileMgr.writeBuffer(PREFIX_SNAPSHOT_INDEX + this.dbName, array, page.pos);
	//					System.out.println("old " + page.pageID() + ", " + page.pos + ", " + page.node.isLeaf());
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
			}
			
			this.dirtyPageList.clear();
			
			b = true;
		}
		
		return b;
	}
	
	public IndexPage getPage(int pageID){
		return this.pageMap.get(pageID);
	}
	
	public int nextNodeID(){
		return this.counter.getAndIncrement();
	}

	public IBTree getBTree(){
		return this.tree;
	}
	
	public void load(){
	
		try {
			List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();
			
			FileDeco fd = this.fileMgr.pinFileChannel(PREFIX_INDEX + this.dbName);
			long len = fd.getFileChannel().size();
			System.out.println(PREFIX_INDEX + this.dbName + " len = " + len);
			if(len >= IndexPage.INT_SIZE){
				fd.getFileChannel().position(0);
				ByteBuffer buff = ByteBuffer.allocate(IndexPage.INT_SIZE);
				fd.getFileChannel().read(buff);
				buff.rewind();
				this.counter.set(buff.getInt());
				
				System.out.println("load counter = " + this.counter.get());
			}
			if(len >= IndexPage.PAGE_SIZE + IndexPage.INT_SIZE){
				int size = (int) ((len - IndexPage.INT_SIZE) / IndexPage.PAGE_SIZE);
				
				System.out.println("load size = " + size + ", len = " + len);
				
				if(size * IndexPage.PAGE_SIZE < len){
					ByteBuffer[] array = new ByteBuffer[size];
					for(int i = 0; i < size; ++i){
						ByteBuffer buffer = this.bufferMgr.pinBuffer();
						array[i] = buffer;
						array[i].position(0);
						bufferList.add(buffer);								
					}
					
					fd.getFileChannel().position(IndexPage.INT_SIZE);
					fd.getFileChannel().read(array);								
				}
			}
			
			List<IndexPage> pageList = new ArrayList<IndexPage>();
			if(null != bufferList && bufferList.size() > 0){

				this.tree = BStarTree.buildBTree(this.dbName, this, this.transMgr, bufferList, pageList);

			}else{

				this.tree = new BStarTree(this.dbName, this, this.transMgr);
/*				IndexPage page = this.appendNewIndexPage(this.tree.getRoot());
				pageList.add(page);*/
			}

			if(pageList.size() > 0){
				for(IndexPage page : pageList){
//					System.out.println("load " + page.pageID() + ", " + page.pos);
					this.pageMap.put(page.pageID(), page);
				}
			}		
			
			this.fileMgr.copy(PREFIX_INDEX + this.dbName, PREFIX_SNAPSHOT_INDEX + this.dbName);
		} catch (Exception e) {
			System.out.println(e);
			e.printStackTrace();
		}
		

	}
	
	public void flush(){
	
		if(null != this.tree){
			try {
				List<ByteBuffer> bufferList = new ArrayList<ByteBuffer>();

				this.tree.toByteBuffer(bufferList);

				if(null != bufferList && bufferList.size() > 0){
			
					FileDeco fd = this.fileMgr.pinFileChannel(PREFIX_TEMP_INDEX + this.dbName);
					ByteBuffer[] array = new ByteBuffer[bufferList.size()];
					fd.getFileChannel().truncate(0);
					fd.getFileChannel().position(0);
					
					ByteBuffer buff = ByteBuffer.allocate(IndexPage.INT_SIZE);
					buff.putInt(this.counter.get());
					buff.position(0);
					System.out.println("flush counter = " + this.counter.get());
					fd.getFileChannel().write(buff);
			
					for(int i = 0; i < bufferList.size(); ++i){
						array[i] = bufferList.get(i);
			//			System.out.println(i + " limit = " + array[i].limit() + ",  size = " + array[i].getInt(17));
						array[i].position(0);
						
						while(array[i].hasRemaining()) {
							fd.getFileChannel().write(array[i]);
						}
					}
					
				    this.fileMgr.closeFileChannel(fd);			
				    this.fileMgr.delete(PREFIX_BAK_INDEX + this.dbName);
				    
				    this.fileMgr.rename(PREFIX_INDEX + this.dbName, PREFIX_BAK_INDEX + this.dbName);
					this.fileMgr.rename(PREFIX_TEMP_INDEX + this.dbName, PREFIX_INDEX + this.dbName);
				}
			} catch (IOException e) {
				System.out.println(e);
				e.printStackTrace();
			}
		}		
	}
}
