package com.hdfs.namenode;

import java.io.IOException;
import java.util.HashMap;
import java.util.Vector;
import com.hdfs.datanode.*;
import com.hdfs.miscl.Constants;

public class PopulateBlocks {

	public static HashMap<String,Integer> allBlocksHashMap;
	
	public PopulateBlocks()
	{
		allBlocksHashMap = new HashMap<>();
	}
	
	public HashMap<String,Integer> returnAllBlocks()
	{
		/**Scan all files in the Directory file and gather each block **/
		Vector<String> myFiles = new Vector<>();
		FileReaderClass myFileReader = new FileReaderClass(Constants.NAME_NODE_CONF_NEW);
		myFileReader.openFile();
		
		String line;
		try {
			line = myFileReader.buff_reader.readLine();
			while(line!=null)
			{
				myFiles.add(line);
				line = myFileReader.buff_reader.readLine();
			}
			
		/** now files are all populated, time to put them in a hashMap and return it**/
			myFileReader.closeFile();
			
			
			for(int i=0;i<myFiles.size();i++)
			{
				addBlocksFromFile(myFiles.get(i));
			}
			
			/**clear all the file names **/
			myFiles.clear();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		return allBlocksHashMap;
	}
	/**
	 * block format is 12.1.1 change to 12.1,  remove the clock
	 * @param fileName
	 */
	public static void addBlocksFromFile(String fileName)
	{
		
		FileReaderClass myFileReader = new FileReaderClass(Constants.PREFIX_DIR+fileName);//File/filename
		myFileReader.openFile();
		String line;
		try {
			line = myFileReader.buff_reader.readLine();
			while(line!=null)
			{
				String[] blockWithVersion = line.split("\\.");
				allBlocksHashMap.put(blockWithVersion[0], Integer.valueOf(blockWithVersion[1]));//changed 12->1
				line = myFileReader.buff_reader.readLine();
			}		
		
			myFileReader.closeFile();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
