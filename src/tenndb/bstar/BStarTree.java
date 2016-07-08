package tenndb.bstar;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.RefVar;
import tenndb.index.IBTree;
import tenndb.index.IndexMgr;
import tenndb.index.IndexPage;
import tenndb.log.CellLogMgr;
import tenndb.tx.AbortTransException;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;


public class BStarTree implements IBTree {

	public final static int BALANCE_SIZE = 256;
	
	protected String    dbName;
	protected final int size;
	protected BTreeNode root;
	protected IndexMgr  indexMgr;
	protected TransMgr  transMgr;
	
	protected ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	public void lockRead()    { this.lock.readLock().lock(); }
	
	public void unLockRead()  { this.lock.readLock().unlock(); }

	public void lockWrite()   { this.lock.writeLock().lock(); }
	
	public void unLockWrite() { this.lock.writeLock().unlock(); }
	
	
	public BStarTree(String dbName, IndexMgr indexMgr, TransMgr transMgr) {
		super();
		this.dbName   = dbName;
		this.size     = BALANCE_SIZE;
		this.indexMgr = indexMgr;
		this.transMgr = transMgr;
	}
	
	public BStarTree(String dbName, IndexMgr indexMgr, TransMgr transMgr, BTreeNode root) {
		super();
		this.dbName   = dbName;
		this.size     = BALANCE_SIZE;
		this.indexMgr = indexMgr;
		this.transMgr = transMgr;		
		this.root     = root;
	}
	
	public BTreeNode getRoot() { return this.root; }
	
	public static IBTree buildBTree(String dbName, IndexMgr indexMgr, TransMgr tansMgr, List<ByteBuffer> buffList, List<IndexPage> pageList){
		IBTree tree = null;
		////type + pageid + parentid + priorid + nextid + size + (key + offset) * BALANCE_SIZE
		if(null != buffList && buffList.size() > 0){
			Map<Integer, BTreeNode> map = new HashMap<Integer, BTreeNode>();
			List<BTreeNode> nodeList    = new ArrayList<BTreeNode>();
			BTreeNode rootNode = null;
			for(int i = 0; i < buffList.size(); ++i){
				ByteBuffer buffer = buffList.get(i);
//				System.out.println("buildBTree.1 " + i + " " + buffer.limit());	
				if(buffer.limit() == IndexPage.PAGE_SIZE){
								
					buffer.position(0);
					byte type 	 = buffer.get(0);
					int pageID   = buffer.getInt(1);
					int parentID = buffer.getInt(5);
					int priorID  = buffer.getInt(9);
					int nextID 	 = buffer.getInt(13);
					int size 	 = buffer.getInt(17);

					int[] keys = new int[size];
					IdxBlock[] values = new IdxBlock[size + 2];
					BTreeNode[] children = null;
					
					boolean isLeaf = false;
					if(BTreeNode.NODE_LEAF == type || (BTreeNode.NODE_ROOT == type && 1 == buffList.size())){
						values = new IdxBlock[size];
						for(int t = 0; t < size; ++t){
							int key 	 = buffer.getInt(IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t);
							int pid 	 = buffer.getInt(IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t + 4);
							int offset 	 = buffer.getInt(IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t + 8);
							int time 	 = buffer.getInt(IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t + 12);
							byte tag     = buffer.get   (IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t + 16);
							
							keys[t] = key;
							values[t]        = new IdxBlock(key);
							values[t].key    = key;
							values[t].pageID = pid;
							values[t].offset = offset;
							values[t].time   = time;
							values[t].tag    = tag;
						}
						isLeaf = true;
					}else if(BTreeNode.NODE_BRANCH== type || (BTreeNode.NODE_ROOT == type && 1 < buffList.size())){
						for(int t = 0; t < size + 2; ++t){
							int key 	 = buffer.getInt(IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t);
							int pid 	 = buffer.getInt(IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t + 4);
							byte tag     = buffer.get   (IndexPage.PAGE_HEAD_SIZE + IndexPage.PAGE_BLOCK_SIZE * t + 16);
							
							if(t < size)
								keys[t] = key;
							values[t]        = new IdxBlock(key);
							values[t].key    = key;
							values[t].pageID = pid;
							values[t].offset = 0;
							values[t].time   = 0;							
							values[t].tag    = tag;
						}
						isLeaf = false;;
					}
					
					//(boolean isLeaf, int num, int[] keys, Object[] values, BTreeNode[] children, BTreeNode parent, BTreeNode next, BTreeNode prior)
					BTreeNode node = new BTreeNode(dbName, isLeaf, size, keys, values, children, null, null, null);
					node.pageID   = pageID;
					node.parentID = parentID;
					node.priorID  = priorID;
					node.nextID   = nextID;
					
					IndexPage page = indexMgr.addIndexPage(node, buffer);
					long pos = IndexPage.PAGE_SIZE * i + IndexPage.INDEX_HEAD_SIZE;
					page.setPos(pos);
					node.setRefPage(page);
					pageList.add(page);
					
					if(!isLeaf)
						node.values = values;
					
					nodeList.add(node);
					map.put(pageID, node);
					
					if(BTreeNode.NODE_ROOT == type){

						rootNode = node;
					}
				}
			}
						
			for(int i = 0; i < nodeList.size(); ++i){
				BTreeNode node = nodeList.get(i);
				
				node.parent = map.get(node.parentID);
				node.prior  = map.get(node.priorID);
				node.next   = map.get(node.nextID);

				if(!node.isLeaf){

					for(int t = 0; t < node.numKeys + 2; ++t){
						node.children[t] = map.get(node.values[t].pageID);
					}			
					node.values = null;
				}		
			}
			
			tree = new BStarTree(dbName, indexMgr, tansMgr, rootNode);
		}
		
		return tree;
	}
	
    public void print(List<String> strList){
    			
		try{
			this.lockRead();			
			if( root != null ){
				root.toString(strList);
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
    }
	
    public String toString(){
    	String str = null;
        	
		try{
	    	this.lockRead();			
	    	if( root != null ){
	            str = root.toString();
	        }	
	    	
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		
    	return str;
    }
    
    @Override
	public void printTreePrior(){
		
    	try{
        	this.lockRead();
        	
    		if(null != root){				
				BTreeNode currentNode = root;
				while(null != currentNode && !currentNode.isLeaf){
					currentNode = currentNode.children[currentNode.numKeys];
				}
				
				if(null != currentNode){
					int index = 0;
					String str = "";				
					while(null != currentNode){
						
						str = "";
						index++;
						for(int i = currentNode.numKeys - 1; i >= 0 ; --i){
							if(null != currentNode.values[i]){
								str += currentNode.keys[i] + "_" + currentNode.values[i].pageID + ":" + currentNode.values[i].offset + ",";		
							}else{
								str += currentNode.keys[i] + "_null,";
							}
						}
						System.out.println(index + ":" + str);
						
						currentNode = currentNode.prior;
					}
				}			
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}			
	}
	
	@Override
	public void printNext() {
		
		try{			
			this.lockRead();
			
			if(null != root){
				BTreeNode currentNode = root;
				while(null != currentNode && !currentNode.isLeaf){
					currentNode = currentNode.children[0];
				}
				
				if(null != currentNode){
					int index = 0;
					String str = "";				
	
					while(null != currentNode){
						str = "PageID = " + currentNode.getPageID() + ", ";
						index++;
						for(int i = 0; i < currentNode.numKeys; ++i){
							if(null != currentNode.values[i]){
								str += currentNode.keys[i] + "_" + currentNode.values[i].pageID + ":" + currentNode.values[i].offset + ",";								
							}else{
								str += currentNode.keys[i] + "_null,";
							}
						}
						System.out.println(index + ":" + str);
						
						currentNode = currentNode.next;
					}
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}		
	}
	
	protected boolean addChild(BTreeNode parentNode, int key, BTreeNode newNode){
		boolean b = false;
		
		b = parentNode.addChild(key, newNode);
		
		if(true == b){
			this.indexMgr.appendNewIndexPage(null, parentNode);
		}
		
		return b;
	}

	@Override
	public IdxBlock insert(int key, IdxBlock value, Trans tid, CellLogMgr logMgr, RefVar t) throws AbortTransException  {
		IdxBlock oldVal = null;		
		try{			
			long t9 = System.currentTimeMillis();
			this.lockRead();
			long t10 = System.currentTimeMillis();
			t.var5 += (t10 - t9);
			
			if(null!= root){
				long t13 = System.currentTimeMillis();
				BTreeNode currentNode = root;
				while(null != currentNode && !currentNode.isLeaf){
					currentNode = currentNode.getChild(key);
				}
				long t14 = System.currentTimeMillis();
				t.var7 += (t14 -t13);

				if(null != currentNode){
					BTreeNode leaf = currentNode;
					long t5 = System.currentTimeMillis();
					InsertVar insert = leaf.addValue(key, value, this.transMgr, tid, logMgr);
					long t6 = System.currentTimeMillis();
					t.var3 += (t6 - t5);
					
					if(false == insert.conflict){
						if(!insert.inserted){
							
							long t7 = System.currentTimeMillis();
							this.unLockRead();
							this.lockWrite();
							long t8 = System.currentTimeMillis();
							t.var4 += (t8 - t7);
							
							long t1 = System.currentTimeMillis();
							SplitLeafVar slVar = leaf.splitLeaf(key, value, this.transMgr, tid, logMgr);
							long t2 = System.currentTimeMillis();
							t.var1 += (t2 - t1);
							BTreeNode newRight = slVar.newRight;
						
							this.indexMgr.appendNewIndexPage(newRight, null);
						
							oldVal = slVar.oldVar.oldValue;
							
							BTreeNode parent = newRight.parent;
							int addToParent  = newRight.lowerBound();
							
							while(null != parent && ! this.addChild(parent,addToParent, newRight)){

								long t3 = System.currentTimeMillis();
								BTreeNode parentRight = parent.splitBranch(addToParent, newRight); 	
								long t4 = System.currentTimeMillis();
								t.var2 += (t4 - t3);
								this.indexMgr.appendNewIndexPage(parentRight, parent);

								
								
			                    addToParent = parent.getMiddle();
								parent      = parent.parent;
								newRight    = parentRight;
							}

							if(null == parent){

								BTreeNode newRoot = new BTreeNode(this.dbName, root, newRight, addToParent);
								this.indexMgr.appendNewIndexPage(newRoot, root);
								
								root.parent     = newRoot;
								newRight.parent = newRoot;
								root            = newRoot;					
							}	
							
							this.unLockWrite();
							this.lockRead();
						}else{
							oldVal = insert.oldValue;
							this.indexMgr.appendNewIndexPage(null, leaf);
						}
					}else{
						throw new AbortTransException("insert: key = " + key);
					}				
				}
			}else{				
				this.unLockRead();
				this.lockWrite();
				
				this.root = new BTreeNode(this.dbName, key, value, this.transMgr, tid);
				this.indexMgr.appendNewIndexPage(root, null);
				
				this.unLockWrite();
				this.lockRead();
			}
			
	//		long t11 = System.currentTimeMillis();
	//		this.indexMgr.flushNewPages();
	//		long t12 = System.currentTimeMillis();
	//		t.var6 += (t12 - t11);
			
		}catch(AbortTransException ae){
			System.out.println(ae);
			throw ae;
		}
		catch(Exception e){
			System.out.println(e);
		}
		finally{
			this.unLockRead();
		}	
		
		return oldVal;
	}

	@Override
	public IdxBlock update(int key, IdxBlock rowData, Trans tid, CellLogMgr logMgr) throws AbortTransException {
		IdxBlock oldObj = null;
		
		try{
			this.lockRead();
			
			BTreeNode currentNode = this.root;
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
			
			if(null != currentNode){
				oldObj = currentNode.setValue(key, rowData, this.transMgr, tid, logMgr);
			}
			
			//		this.indexMgr.flushNewPages();
		}catch(AbortTransException ae){
			System.out.println(ae);
			throw ae;					
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}	
		
		return oldObj;
	}

	@Override
	public IdxBlock delete(int key, Trans tid, CellLogMgr logMgr) {
		IdxBlock oldObj = null;
		
		try{
			this.lockWrite();
			
			BTreeNode currentNode = root;
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
			
			if(null != currentNode){
				oldObj = currentNode.delValue(key, this.transMgr, tid, logMgr);
				if(null != oldObj){
					this.indexMgr.appendNewIndexPage(null, currentNode);
				}
			}
			
	//		this.indexMgr.flushNewPages();
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}	
		
		return oldObj;
	}
	
	@Override
	public BTreeNode seekNode(int key) {
		BTreeNode currentNode = null;
		
		try{
			this.lockRead();
			
			currentNode = this.root;
			
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		return currentNode;
	}
	
	public boolean rollback(int key, Trans tid, CellLogMgr logMgr){
		boolean b = false;
		
		try{
			this.lockRead();
			
			BTreeNode currentNode = this.root;
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
			
			if(null != currentNode && currentNode.isLeaf){
				IdxBlock blk = currentNode.rollback(key, this.transMgr, tid);
				if(null != blk){
		//			blk.idxPageID = currentNode.pageID;
		//			b = this.indexMgr.flushBlock(blk);
					b = true;
				}
			}			
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		return b;
	}	
	
	public boolean commit(int key, Trans tid, CellLogMgr logMgr){
		boolean b = false;
		
		try{
			this.lockRead();
			
			BTreeNode currentNode = root;
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
			
			if(null != currentNode && currentNode.isLeaf){
				IdxBlock blk = currentNode.commit(key, this.transMgr, tid);
				if(null != blk){
					blk.idxPageID = currentNode.pageID;
					b = this.indexMgr.flushBlock(blk);
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		
		return b;
	}	
	
	@Override
	public IdxBlock search(int key, Trans tid) {
		IdxBlock value = null;
	
		try{
			this.lockRead();
			
			BTreeNode currentNode = this.root;
			while(null != currentNode && !currentNode.isLeaf){
				currentNode = currentNode.getChild(key);
			}
			
			if(null != currentNode && currentNode.isLeaf){
				value = currentNode.getValue(key, this.transMgr, tid);
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		
		return value;
	}

	@Override
	public List<IdxBlock> range(int fromKey, int toKey, boolean isInc) {
		List<IdxBlock> list = new ArrayList<IdxBlock>();
		
		try{	
			this.lockRead();			
			if(fromKey <= toKey){
				
				if(isInc){			
					BTreeNode node = this.seekNode(fromKey);
					boolean next = true;
					while(null != node){
						int i = 0;
						for(; i < node.numKeys; ++i){
							if(node.keys[i] >= fromKey && node.keys[i] <= toKey){								
								if(IdxBlock.VALID == node.values[i].tag){
									IdxBlock iblk = new IdxBlock(node.keys[i]);
									iblk.pageID = node.values[i].pageID;
									iblk.offset = node.values[i].offset;
									
									list.add(iblk);
								}								
							}else if(node.keys[i] > toKey){
								next = false;
								break;
							}
						}
						if(next)
							node = node.next;
						else
							node = null;
					}			
				}else{
					
					BTreeNode node = this.seekNode(toKey);
					boolean next = true;
					while(null != node){
						int i = node.numKeys - 1;
						for(; i >= 0; --i){
							if(node.keys[i] >= fromKey && node.keys[i] <= toKey){	
								if(IdxBlock.VALID == node.values[i].tag){
									IdxBlock iblk = new IdxBlock(node.keys[i]);
									iblk.pageID = node.values[i].pageID;
									iblk.offset = node.values[i].offset;
									
									list.add(iblk);
								}
							}else if(node.keys[i] < fromKey){
								next = false;
								break;
							}
						}
						if(next)
							node = node.prior;
						else
							node = null;
					}
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		
		return list;
	}

	@Override
	public void toByteBuffer(List<ByteBuffer> list) {

		if(null != this.root && null != list){

			try{
				this.lockRead();					
				this.root.toByteBuffer(list, this.root);
			}catch(Exception e){
				System.out.println(e);
			}
			finally{
				this.unLockRead();;
			}	
		}			
	}
		
	@Override
	public int count(int fromKey, int toKey) {
		int count = 0;
		
		try{	
			this.lockRead();			
			if(fromKey <= toKey){
				
				BTreeNode node = this.seekNode(fromKey);
				boolean next = true;
				while(null != node){
					int i = 0;
					for(; i < node.numKeys; ++i){
						if(node.keys[i] >= fromKey && node.keys[i] <= toKey){
							if(IdxBlock.VALID == node.values[i].tag){
								++count;
							}							
						}else if(node.keys[i] > toKey){
							next = false;
							break;
						}
					}
					if(next)
						node = node.next;
					else
						node = null;
				}				
			}
		}catch(Exception e){}
		finally{
			this.unLockRead();
		}
		
		return count;
	}

}
