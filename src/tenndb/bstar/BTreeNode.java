package tenndb.bstar;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import tenndb.index.IndexPage;
import tenndb.log.CellLogMgr;
import tenndb.tx.AbortTransException;
import tenndb.tx.BlkID;
import tenndb.tx.Trans;
import tenndb.tx.TransMgr;



public class BTreeNode {
	
	protected final static int NODE_BRANCH = 0; 
	protected final static int NODE_LEAF   = 1;
	protected final static int NODE_ROOT   = 2;
	protected final static int MAX_SIZE    = BStarTree.BALANCE_SIZE;
	protected int[] keys;
    protected BTreeNode[] children;    
	protected IdxBlock[] values;

	protected int numKeys = 0;
	
	protected BTreeNode parent = null;
	protected BTreeNode next = null;
	protected BTreeNode prior = null;
	
	protected String dbName = null;
	
	protected boolean isLeaf = false;
	
	//index page file
//	protected int nodeID   = -1;
	
	protected int pageID   = -1;
	protected int parentID = -1;
	protected int priorID  = -1;
	protected int nextID   = -1;

	public int size() { return this.numKeys; }
	
	public boolean leaf() { return this.isLeaf; }
	
	public IdxBlock getBlock(int index) {
		IdxBlock blk = null;
		
		if(index < this.numKeys){
			blk = this.values[index];
		}
		
		return blk;
	}

	protected IndexPage refPage = null;
	
	protected ReadWriteLock lock = new ReentrantReadWriteLock(false);
	
	protected void lockRead()    { this.lock.readLock().lock(); }
	
	protected void unLockRead()  { this.lock.readLock().unlock(); }

	protected void lockWrite()   { this.lock.writeLock().lock(); }
	
	protected void unLockWrite() { this.lock.writeLock().unlock(); }
	
	public IndexPage getRefPage() {
		return refPage;
	}

	public void setRefPage(IndexPage refPage) {
		this.refPage = refPage;
	}

	public void setPageID(int pageID){
		this.pageID = pageID;
	}
	
	public int getPageID(){
		return this.pageID;
	}
	
	public BTreeNode(String dbName, boolean isLeaf, int num, int[] keys, IdxBlock[] values, BTreeNode[] children, BTreeNode parent, BTreeNode next, BTreeNode prior){
		this.dbName  = dbName;
		this.isLeaf  = isLeaf;		
		this.numKeys = num;
//		System.out.println("num = " + num);
		this.keys = new int[MAX_SIZE + 1];
		
		for(int i = 0; i < numKeys; ++i){
			this.keys[i] = keys[i];
		}
		
		if(this.isLeaf){
			this.values = new IdxBlock[MAX_SIZE + 2];		
				
			for(int i = 0; i < num + 2; ++i){
				if(i < values.length)
					this.values[i] = values[i];
				else
					this.values[i] = null;
			}		
		}else{
			
			this.children = new BTreeNode[MAX_SIZE + 2];
			if(null != children){
				
				for(int i = 0; i < num + 2; ++i){
					if(i < children.length)
						this.children[i] = children[i];
					else
						this.children[i] = null;	
				}
			}
		}
		
		this.parent = parent;
		this.next   = next;
		this.prior  = prior;
	}
	
	public void toByteBuffer(ByteBuffer buffer, BTreeNode rootNode){
		buffer.rewind();

		byte type = (byte)(true == this.isLeaf ? NODE_LEAF : NODE_BRANCH);
		
		if(this == rootNode){
			type = NODE_ROOT;
		}

		buffer.put(type);
		
		buffer.putInt(this.getPageID());
		if(null != this.parent)
			buffer.putInt(this.parent.getPageID());
		else
			buffer.putInt(0);
		
		if(null != this.prior)
			buffer.putInt(this.prior.getPageID());
		else
			buffer.putInt(0);
		
		if(null != this.next)
			buffer.putInt(this.next.getPageID());
		else
			buffer.putInt(0);

		buffer.putInt(this.numKeys);

		if(this.isLeaf){	

			for(int i = 0; i < this.numKeys ; ++i){
				buffer.putInt(this.keys[i]);
				buffer.putInt(this.values[i].pageID);
				buffer.putInt(this.values[i].offset);
				buffer.putInt(this.values[i].time);
				buffer.put(this.values[i].tag);
			}

			for(int i = this.numKeys ; i < MAX_SIZE + 2; ++i){
				buffer.putInt(0);
				buffer.putInt(0);
				buffer.putInt(0);
				buffer.putInt(0);
				buffer.put(IdxBlock.INVALID);
			}	

		}else{				

			for(int i = 0; i < this.numKeys + 1; ++i){
				if(i < this.numKeys + 1)
					buffer.putInt(this.keys[i]);
				else
					buffer.putInt(0);
				buffer.putInt(this.children[i].getPageID());
				buffer.putInt(0);
				buffer.putInt(0);
				buffer.put(IdxBlock.VALID);
			}
			
			for(int i = this.numKeys + 1; i < MAX_SIZE + 2; ++i){
				buffer.putInt(0);
				buffer.putInt(0);
				buffer.putInt(0);
				buffer.put(IdxBlock.INVALID);
			}

		}	
	}
	
	////type + pageid + parentid + priorid + nextid + size + (key + offset) * BALANCE_SIZE
	public void toByteBuffer(List<ByteBuffer> list, BTreeNode rootNode){

		this.lockRead();

		ByteBuffer buffer = ByteBuffer.allocate(IndexPage.PAGE_SIZE);
		buffer.rewind();
		
		this.toByteBuffer(buffer, rootNode);

		list.add(buffer);	
		
		if(false == this.isLeaf){

			if(null != this.children[0]){
				this.children[0].toByteBuffer(list, rootNode);
			}
			
			for(int i = 0; i < this.numKeys ; ++i){
				this.children[i+1].toByteBuffer(list, rootNode);
			}			
		}			

		this.unLockRead();
	}
	
	
	public void toString(List<String> list){

//		this.lockRead();
		
		if(!this.isLeaf){
			if(null != children[0]){
		       children[0].toString(list);  
		       for( int i = 0; i < numKeys; ++i ){
		           children[i+1].toString(list); 
		       }
			}
		}else{
			String output = "[L";
	        for( int i = 0; i < numKeys; ++i ){
	            output += " " + keys[i] + ":" + values[i].pageID + ":" + values[i].offset + ", ";
	        }
	        
			if(null != next)
				 output += "next = "+ next.keys[0] +": ";  
			output += "]";
			list.add(output);
		}	
		
//		this.unLockRead();
	}
	
	
	public String toString(){
		String output = "[L";
		
//		this.lockRead();

		if(!this.isLeaf){
			if(null != children[0]){
		       output += "["+children[0].toString()+"], ";  
		        for( int i = 0; i < numKeys; ++i ){
		            output += keys[i] + ":" + children[i+1].toString(); 
		            if( i < numKeys - 1 ) output += ", ";
		        }
			}
		}else{

	        for( int i = 0; i < numKeys; ++i ){
	            output += " " + keys[i] + ":" + values[i].pageID + ":" + values[i].offset + ", ";
	        }
	        
			if(null != next)
				 output += "next = "+ next.keys[0] +": ";  
		}	
		
//		this.unLockRead();
		
		return output + "]";
	}
	
	public BTreeNode(String dbName, BTreeNode left, BTreeNode right, int key){		
		this.dbName  = dbName;
		this.keys    = new int[MAX_SIZE + 1];
		this.keys[0] = key;
		this.numKeys = 1;
      
		this.children    = new BTreeNode[MAX_SIZE + 2];
		this.children[0] = left;
		this.children[1] = right;

		this.isLeaf      = false;
	}
	
	public BTreeNode(String dbName, int key, IdxBlock obj, TransMgr transMgr, Trans tid){
		this.dbName  = dbName;
		this.keys    = new int[MAX_SIZE + 1];
		this.keys[0] = key;
		this.numKeys = 1;
		
		this.values    = new IdxBlock[MAX_SIZE + 2];
		this.values[0] = obj;
		this.isLeaf    = true;
			
		if(null != transMgr && null != tid && tid.getTransID() > 0){
			
			if(transMgr.lockBlk(new BlkID(this.dbName, key), tid)){
//				System.out.println("addValue.0 " + key);
				this.values[0].drity = true;
				this.values[0].tid   = tid.getTransID();
				this.values[0].tag   = IdxBlock.INVALID;
				
				transMgr.setTransState(tid, Trans.ACTIVE);
			}else{
				transMgr.setTransState(tid, Trans.ROLLBACK);
			}
		}
	}
	
	public boolean isLeaf() {
		return isLeaf;
	}

	public void setLeaf(boolean isLeaf) {
		this.isLeaf = isLeaf;
	}
	
	public int getMiddle(){
		return this.keys[keys.length/2];
	}
	
	public int lowerBound(){
		return this.keys[0];
	}
	
	public int upperBound(){
		return this.keys[this.numKeys - 1];
	}
	
	public BTreeNode splitBranch(int key, BTreeNode node){
		BTreeNode newNode = null;
		
		try{
			this.lockWrite();
			if(!this.isLeaf){

				if(this.numKeys == MAX_SIZE){

					int i = 0;
					for(; i < MAX_SIZE;){
						if(key >= this.keys[i])
							++i;
						else
							break;
					}

					if(this.keys[i] != key){						
						for(int t = MAX_SIZE; t > i; --t){
							this.keys[t] = this.keys[t-1];
							this.children[t + 1] = this.children[t];
						}
						
						this.keys[i] = key;
						this.children[i + 1] = node;

						int[] newKeys = new int[MAX_SIZE + 1];
						BTreeNode[] newChildren = new BTreeNode[MAX_SIZE + 2];
						
						{
							int from = (MAX_SIZE + 1)/2 + 1;
							int to   = MAX_SIZE + 1;
							int newLength = to - from; 
							
							for(int t = 0; t < newLength; ++t){
					            if( t + from < to ) {
					            	newKeys[t] = this.keys[from+t];
					            } 
					            else{
					            	newKeys[t] = -1;
					            }
							}
						}
						
						{
							int from = (MAX_SIZE + 2 + 1)/2;
							int to   = MAX_SIZE + 2;
							int newLength = to - from; 
							
							for(int t = 0; t < newLength; ++t){
					            if( t + from < to ) {
					            	newChildren[t] = this.children[from+t];
					            }else{
					            	newChildren[t] = null;
					            }
							}
						}
						
						int newNum = (MAX_SIZE + 1)/2;
						newNode = new BTreeNode(this.dbName, false, newNum, newKeys, null, newChildren, this.parent, this.next, this);
				        for( int t = 0; t <= newNum ; ++t ) {
				        	if(null != newNode.children[t])
				        		newNode.children[t].parent = newNode;
				        }
				        
				        this.numKeys = (MAX_SIZE + 1)/2;
						this.next = newNode;
					}				
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}
		
		return newNode;
	}
	
		
	public SplitLeafVar splitLeaf(int key, IdxBlock value, TransMgr transMgr, Trans tid, CellLogMgr logMgr){

		SplitLeafVar slVar = new SplitLeafVar();
		
		try{
			this.lockWrite();			
			if(this.isLeaf){
				int from = MAX_SIZE/2;
				int to   = MAX_SIZE;
				int newLength = to - from;
				
				int[] newKeys = new int[MAX_SIZE + 1];
				IdxBlock[] newValues = new IdxBlock[MAX_SIZE + 2];
				
				for(int i = 0; i < newLength; ++i){
		            if( i + from < to ) {
		            	newKeys[i]   = this.keys[from+i];
		            	newValues[i] = this.values[from+i];
		            } 
				}

				slVar.newRight = new BTreeNode(this.dbName, true, MAX_SIZE/2, newKeys, newValues, null, this.parent, this.next, this);
				this.next = slVar.newRight;
				this.numKeys = MAX_SIZE/2;
				
				if(key >= slVar.newRight.lowerBound()){
					slVar.oldVar = slVar.newRight.addValue(key, value, transMgr, tid, logMgr);
				}else{
					slVar.oldVar = this.addValue(key, value, transMgr, tid, logMgr);
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}

		return slVar;
	}
	
	public boolean addChild(int key, BTreeNode node){
		boolean b = false;

		try{
			    	
			this.lockWrite();
			if(!this.isLeaf && this.numKeys < MAX_SIZE){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
				
				if(i < MAX_SIZE){
					
					for(int t = this.numKeys; t > i; --t){
						this.keys[t] = this.keys[t - 1];
						this.children[t+1] = this.children[t];
					}
					
					this.keys[i] = key;
					this.children[i+1] = node;
					this.numKeys++;
					
					b = true;
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}

		return b;
	}
	
	public InsertVar addValue(int key, IdxBlock newValue, TransMgr transMgr, Trans tid, CellLogMgr logMgr){

		InsertVar var = new InsertVar();
		var.inserted = true;
		var.oldValue = null;
		
		try{
			this.lockWrite();
						
			if(this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
				
				if(i != this.numKeys && this.keys[i] == key){
//					System.out.println("addValue.1 " + key);
					if(null != transMgr && null != tid && tid.getTransID() > 0){
						BlkID bid = new BlkID(this.dbName, key);
						
//						System.out.println("addValue.2 " + key);
						var.inserted = false;
						logMgr.logUpdate(tid, key, newValue, this.values[i]);
						if(transMgr.lockBlk(bid, tid)){							
							
//							System.out.println("addValue.3 " + key);
							var.oldValue   = this.values[i];
							this.values[i] = newValue;
							var.inserted   = true;
							
							this.values[i].drity   = true;
							this.values[i].tid     = tid.getTransID();
							this.values[i].tag     = IdxBlock.VALID;
							this.values[i].newTag  = IdxBlock.VALID;
							this.values[i].old     = var.oldValue;
							transMgr.setTransState(tid, Trans.ACTIVE);
						}else{
							transMgr.setTransState(tid, Trans.ROLLBACK);
							var.conflict   = true;
						}
					}else{
						if(false == this.values[i].drity){
							
							var.oldValue   = this.values[i];
							this.values[i] = newValue;
							var.inserted   = true;
							
							this.values[i].tag   = IdxBlock.VALID;						
						}else{
							var.inserted = false;
						}
					}
				}else if(this.numKeys != MAX_SIZE){
					
					for(int t = this.numKeys; t > i; --t){
						this.keys[t]   = this.keys[t - 1];
						this.values[t] = this.values[t - 1];
					}
								
					if(null != transMgr && null != tid && tid.getTransID() > 0){
						
						var.inserted = false;
						
						logMgr.logInsert(tid, key, newValue);
						
						if(transMgr.lockBlk(new BlkID(this.dbName, key), tid)){
														
							this.keys[i]   = key;
							this.values[i] = newValue;
							
//							System.out.println("addValue.0 " + key);
							this.values[i].drity  = true;
							this.values[i].tid    = tid.getTransID();
							this.values[i].old    = null;
							this.values[i].tag    = IdxBlock.INVALID;
							this.values[i].newTag = IdxBlock.VALID;
							
							this.numKeys++;	
							var.inserted = true;
			
						}else{
		
							var.conflict = true;
						}
					}else{
//						System.out.println("addValue.0 " + key);
						this.keys[i]   = key;
						this.values[i] = newValue;
						
						this.numKeys++;	
						var.inserted = true;
					}
				}else{
					var.inserted = false;
				}
			}			
		}catch(Exception e){}
		finally{
			this.unLockWrite();			
		}

		return var;
	}

	public BTreeNode getChild(int key){
		BTreeNode child = null;
		
		try{
			this.lockRead();
			
			if(!this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] <= key)
						++i;
					else
						break;
				}
				child = children[i];
			}
		}
		catch(Exception e){
			System.out.println(e);
		}
		finally{
			this.unLockRead();
		}
		
		return child;
	}
	
	public IdxBlock delValue(int key, TransMgr transMgr, Trans tid, CellLogMgr logMgr) throws AbortTransException{
		IdxBlock oldObj = null;
		
		try{			
			this.lockWrite();
		
			if(this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
	
				if(this.keys[i] == key && IdxBlock.VALID == this.values[i].tag){
										
					if(null != transMgr && null != tid && tid.getTransID() > 0){	
						logMgr.logDelete(tid, key);
						
						if((false == this.values[i].drity)
						 ||(this.values[i].drity && this.values[i].tid == tid.getTransID())){
							
							if(transMgr.lockBlk(new BlkID(this.dbName, key), tid)){
								oldObj = this.values[i];
								
								this.values[i].drity   = true;
								this.values[i].tid     = tid.getTransID();
								this.values[i].tag     = IdxBlock.VALID;
								this.values[i].newTag  = IdxBlock.INVALID;
								
								this.values[i].old     = oldObj;
								transMgr.setTransState(tid, Trans.ACTIVE);
							}else{
								transMgr.setTransState(tid, Trans.ROLLBACK);
							}			
						}else{
							transMgr.setTransState(tid, Trans.ROLLBACK);
							tid.setState(Trans.ROLLBACK);
							////////// conflict
							throw new AbortTransException("delValue: key = " + key);
						}
					}else{
						if(false == this.values[i].drity){
							this.values[i].tag = IdxBlock.INVALID;
						}
					}
				}
			}
		}
		catch(AbortTransException ate){
			throw ate;
		}
		catch(Exception e){
			System.out.println(e);
		}
		finally{
			this.unLockWrite();		
		}
		
		return oldObj;
	}
	
	public IdxBlock setValue(int key, IdxBlock newObj, TransMgr transMgr, Trans tid, CellLogMgr logMgr) throws AbortTransException{
		IdxBlock oldObj = null;
				
		try{			
			this.lockWrite();
			
			if(this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
				
				if(this.keys[i] == key){
					
					if(null != transMgr && null != tid && tid.getTransID() > 0){	
						
						logMgr.logUpdate(tid, key, newObj, this.values[i]);
						
						if((false == this.values[i].drity)||
						   (this.values[i].tid == tid.getTransID())){
										
							if(transMgr.lockBlk(new BlkID(this.dbName, key), tid)){
								oldObj = this.values[i];
								this.values[i] = newObj;
								
								this.values[i].drity   = true;
								this.values[i].tid     = tid.getTransID();
								this.values[i].tag     = IdxBlock.VALID;
								this.values[i].newTag  = IdxBlock.VALID;
								this.values[i].old     = oldObj;
								transMgr.setTransState(tid, Trans.ACTIVE);
							}else{
								transMgr.setTransState(tid, Trans.ROLLBACK);
							}
						}else{
							transMgr.setTransState(tid, Trans.ROLLBACK);
							tid.setState(Trans.ROLLBACK);			
							////////// conflict
							throw new AbortTransException("setValue: key = " + key);
						}
					}else{
						if(false == this.values[i].drity){
							oldObj = this.values[i];
							this.values[i] = newObj;
						}
					}
				}
			}
		}catch(AbortTransException ate){
			throw ate;
		}catch(Exception e){
			System.out.println(e);
		}finally{
			this.unLockWrite();		
		}
		
		return oldObj;
	}
	
	public IdxBlock rollback(int key, TransMgr transMgr, Trans tid){
		IdxBlock value = null;
		
		try{			
			this.lockWrite();
			
			if(this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
				
				if(this.keys[i] == key){
					if(null != transMgr && null != tid && tid.getTransID() > 0){
						if((true == this.values[i].drity)&&
						   (this.values[i].tid == tid.getTransID())){
							
							IdxBlock old = this.values[i].old;
							if(null != old){
								this.values[i]       = old;
								this.values[i].old   = null;
								this.values[i].tid   = 0;
								this.values[i].drity = false;
								
							}else{
								this.values[i].tag   = IdxBlock.INVALID;
								this.values[i].tid   = 0;
								this.values[i].drity = false;
							}
							
							value = this.values[i].copyData();							
							value.idxOffset = i;
						}
					}
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}
			
		return value;
	}
	
	public IdxBlock commit(int key, TransMgr transMgr, Trans tid){
		IdxBlock value = null;
		
		try{			
			this.lockWrite();
			
			if(this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
				
				if(this.keys[i] == key){
					if(null != transMgr && null != tid && tid.getTransID() > 0){
						if((false == this.values[i].drity)||
						   (this.values[i].tid == tid.getTransID())){
							
							this.values[i].old   = null;
							this.values[i].drity = false;
							this.values[i].tag   = this.values[i].newTag;
							this.values[i].tid   = 0;
							
							value = this.values[i].copyData();							
							value.idxOffset = i;
						}
					}
				}
			}
		}catch(Exception e){}
		finally{
			this.unLockWrite();
		}
			
		return value;
	}
	
	public IdxBlock getValue(int key, TransMgr transMgr, Trans tid){
		IdxBlock value = null;
				
		try{
			
			this.lockRead();
			if(this.isLeaf){
				int i = 0;
				for(; i < this.numKeys; ){
					if(this.keys[i] < key)
						++i;
					else
						break;
				}
				
//				System.out.println("getValue.1 i = " + i + ", " + this.keys[i]);
				if(this.keys[i] == key && IdxBlock.VALID == this.values[i].tag)
				{
//					System.out.println("getValue.2 i = " + i + ", " + this.keys[i]);
					if(null != transMgr && null != tid && tid.getTransID() > 0){
						if((false == this.values[i].drity)
						 ||(this.values[i].tid == tid.getTransID())){
							
							this.values[i].tid = tid.getTransID();
							value = this.values[i].copyData();
						}else{
							if(null != this.values[i].old){
								value = this.values[i].old.copyData();
							}
						}
					}else{
						if(false == this.values[i].drity){
//							System.out.println("getValue.3 i = " + i + ", " + this.keys[i]);
							value = this.values[i].copyData();
//							System.out.println("getValue.4 " + value.idxPageID + ", " + value.idxOffset + ", " + value.pageID + ", " + value.offset);
						}else{
							if(null != this.values[i].old){
								value = this.values[i].old.copyData();
							}
						}
					}
				}
			}
		}catch(Exception e){
			System.out.println(e);
		}
		finally{
			this.unLockRead();
		}
		return value;
	}
}
