package tenndb.common;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;


public class FileMgr {

	protected ConcurrentMap<String, FileDeco> fileMap = null;

	protected File dir;

//	static int LEN = 0x800000; // 128 Mb  
	  
	public FileMgr(String instName){
		if(null != instName && instName.length() > 0){
			dir = new File(instName);
		}else{
			String homedir = System.getProperty("user.home");
			dir = new File(homedir, instName);
		}
		
		if(!dir.exists()){
			
			FileUtil.mkDir(dir);
			
			if(!dir.exists()){
				throw new RuntimeException("cannot create " + instName);
			}
		}
		
		this.fileMap = new ConcurrentHashMap<String, FileDeco>();
	}
	
	public boolean delete(String fileName){
		boolean b = false;
		this.closeFileChannel(fileName);	
		
		File file = new File(this.dir, fileName);
		if(file.exists()){
			b = file.delete();
		}
		
		if(b){
			System.out.println("delete.ok " + fileName);
		}else{
			System.out.println("delete.failed " + fileName);
		}
		
		return b;
	}
	
	public boolean copy(String oldFileName, String newFileName){
		boolean b = false;

		try {
			long size = this.size(oldFileName);
			if(size > 0){
				this.delete(newFileName);
				FileDeco dest = pinFileChannel(newFileName);
				FileDeco src = pinFileChannel(oldFileName);
				if(null != src && null != dest){
					src.fc.position(0);
					dest.fc.position(0);
					ByteBuffer buffer = ByteBuffer.allocate(1000);
					while(src.fc.position() < src.fc.size() && src.fc.size() > 0){

						src.fc.read(buffer);
						buffer.flip();
						while(buffer.hasRemaining()){
							dest.fc.write(buffer);							
						}
//						System.out.println(src.fc.position() + "," + src.fc.size() + "," + buffer.limit());
						buffer.rewind();
					}
					b = true;
				}
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
		return b;
	}
	
	public long append(String fileName, ByteBuffer buffer){
		long pos = 0;		
		try {
			FileDeco dest = pinFileChannel(fileName);
			if(null != dest){
				pos = dest.fc.size();
				dest.fc.position(dest.fc.size());

				buffer.position(0);

				while(buffer.hasRemaining()){
					dest.fc.write(buffer);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return pos;
	}
	
	public boolean rename(String oldFileName, String newFileName){
		boolean b = false;
		this.closeFileChannel(oldFileName);	
		this.closeFileChannel(newFileName);	
		
		File srcFile = new File(this.dir, oldFileName);
		File destFile = new File(this.dir, newFileName);
		
		b = srcFile.renameTo(destFile);
		if(b){
			System.out.println("rename.ok " + oldFileName + " is renamed to " + newFileName);
		}else{
			System.out.println("rename.failed " + oldFileName + " is renamed to " + newFileName);
		}
		return b;
	}
	
	public void readBuffer(String fileName, byte[] buffer, long pos)  throws IOException{
		File file = new File(this.dir, fileName);
		RandomAccessFile f = new RandomAccessFile(file, "rws");

		f.seek(pos);
		f.read(buffer);
		f.close();
		
//		f.close();
/*		FileDeco fd = pinFileChannel(fileName);
		if(null != fd){
			if(pos + len <= fd.fc.size()){
				fd.fc.position(pos);
				fd.fc.read(buffer);
			}			
		}	*/	
	}
	
	public void writeBuffer(String fileName, byte[] buffer, long pos) throws IOException{
		
		File file = new File(this.dir, fileName);
		RandomAccessFile f = new RandomAccessFile(file, "rws");

		f.seek(pos);
		f.write(buffer);
		f.close();
		
/*		FileDeco fd = pinFileChannel(fileName);
		if(null != fd){
			if(pos + len <= fd.fc.size()){
				fd.fc.position(pos);
				while(buffer.hasRemaining()){
					fd.fc.write(buffer);
				}	
			}			
		}*/
	}
	
	public void removeFileChannel(String fileName) throws IOException{
		
		FileDeco fd = null;
		fd = this.fileMap.remove(fileName);
		
		if(null != fd){
			this.closeFileChannel(fd);
		}			
	}
	
	public void closeFileChannel(FileDeco fd){		
		if(null != fd){
			if(null != fd.fc){
				try {

					fd.fc.close();	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			try {
				this.removeFileChannel(fd.fileName);
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}
	
	public void closeFileChannel(String fileName){	
		FileDeco fd = null;
		
		fd = this.fileMap.get(fileName);
		
		if(null != fd){
			if(null != fd.fc){
				try {

					fd.fc.close();	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

			try {
				this.removeFileChannel(fd.fileName);
			} catch (IOException e) {
				e.printStackTrace();
			}			
		}
	}
	
	
	public FileDeco pinFileChannel(String fileName) throws IOException{		
		FileDeco fd = null;
		
		fd = this.fileMap.get(fileName);
		
		if(null == fd){
			File file = new File(this.dir, fileName);
			RandomAccessFile f = new RandomAccessFile(file, "rws");
			fd = new FileDeco();
			fd.fileName = fileName;
	
			fd.fc = f.getChannel();
			
		//	fd.out = fd.fc.map(FileChannel.MapMode.READ_WRITE, 0, LEN);  
			
			this.fileMap.put(fileName, fd);
		}
		
		return fd;
	}
	
	public synchronized long size(String filename) {
		long size = 0;
		try {
			FileDeco fd = pinFileChannel(filename);
			if(null != fd){
				size = fd.fc.size();
			}
		} catch (IOException e) {
			throw new RuntimeException("cannot access " + filename);
		}
		return size;
	}
}
