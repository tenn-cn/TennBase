package tenndb.common;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class FileUtil
{
	public final static void mkDir(File file) 
	{
		if (file.getParentFile().exists()) {
			file.mkdir();
		} else {
			mkDir(file.getParentFile());
			file.mkdir();
		}
	}
		 
	public final static boolean createFile(File file)
	{
		boolean isCreated = false;
		if (file == null)
		{
			throw new RuntimeException(" can not create null file.");
		}

		if (file.exists())
			isCreated = true;
		else
		{
			File parentDir = file.getParentFile();

			if (parentDir != null && !parentDir.exists())
			{
				if (!parentDir.mkdirs())
				{
					isCreated = false;
				}
			}
			try
			{
				isCreated = file.createNewFile();
			} catch (IOException e)
			{
				throw new RuntimeException(e);
			}
		}
		return isCreated;
	}
	 
	public static boolean deleteDirectory(File path) 
	{
		if (path.exists()) 
		{
			File[] files = path.listFiles();
			for (int i = 0; i < files.length; i++) 
			{
				if (files[i].isDirectory()) 
				{
					deleteDirectory(files[i]);
				} else 
				{
					files[i].delete();
				}
			}
		}
		return (path.delete());
	}  

	public final static  boolean deleteFile(String filePath)
	{
		boolean ret = false;
		
		if(null != filePath && filePath.length() > 0)
		{
			File file = new File(filePath);
			
			if(file.exists())
			{
				ret = file.delete();
			}
		}
		return ret;
	}
	
	public static final void writeFile(String fullFilePath, List<String> list)
	{
		if(null != list && list.size() > 0)
		{
			File file = null;
			FileWriter fw = null;
			BufferedWriter out = null;
			try 
			{
				file = new File(fullFilePath);

				
				fw = new FileWriter(file, true);
				out = new BufferedWriter(fw);
				for (String str : list) 
				{
					if(null != str && str.length() > 0)
					{
						out.append(str);
						out.newLine();	
					}				
				}
				out.flush();
			}
			catch (Exception e) 
			{
				System.out.println("fullFilePath = " + fullFilePath);
				e.printStackTrace();
			} 
			finally
			{
				if(null != fw){
					try {
						fw.close();
					} catch (IOException e) {
					}
				}
				
				if(null != out){
					try {
						out.close();
					} catch (IOException e) {
					}
				}
			}
		}
	}
	
/*	public final static boolean append(String path, String content)
	{
		boolean b = false;
		
		if(null != path && path.length() > 0 
		&& null != content && content.length() > 0 )
		{
			File file = new File(path);
							
			if(createFile(file))
			{
				RandomAccessFile accesser = null;
				try
				{
					accesser = new RandomAccessFile(file, "rw");
											
					accesser.seek(accesser.length());
						
					accesser.writeUTF(content);
					b = true;			
				} catch (Exception e)
				{
					throw new RuntimeException(e);
				} 
				finally
				{
					if(null != accesser)
						try
						{
							accesser.close();
						} catch (IOException e)
						{
						}
				}
			}
		}
		
		return b;
	}
	
	public final static boolean append(String path, byte[] buf, int offset, int len)
	{
		boolean isAppended = false;
		
		if(null != path && path.length() > 0 && null != buf && buf.length > 0 && len > 0)
		{
			File file = new File(path);
			
			//	System.out.println(path);
				
				if(createFile(file))
				{
					RandomAccessFile accesser = null;
					try
					{
						accesser = new RandomAccessFile(file, "rw");
						accesser.seek(accesser.length());
						accesser.write(buf, offset, len);
						isAppended = true;			
					} catch (Exception e)
					{
						throw new RuntimeException(e);
					} 
					finally
					{
						if(null != accesser)
							try
							{
								accesser.close();
							} catch (IOException e)
							{
							}
					}
				}
		}
		return isAppended;
	}*/
}
