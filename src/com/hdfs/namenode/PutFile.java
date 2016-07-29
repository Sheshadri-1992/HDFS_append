package com.hdfs.namenode;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;
import com.hdfs.datanode.*;

import com.hdfs.miscl.Constants;

public class PutFile {

	public HashMap<Integer,String> fileHandletoFileName;   // fileHandle ,fileName 
	public HashMap<Integer,Vector<String>> fileBlocks;   // fileHandle , blocksAssigned so far
	
	public PutFile() {
		// TODO Auto-generated constructor stub
		fileHandletoFileName = new HashMap<>();
		fileBlocks = new HashMap<>();
		
	}
	
	public void insertFileHandle(String fileName,int handle)
	{
		fileHandletoFileName.put(handle,fileName);
		fileBlocks.put(handle,new Vector<String>());
	}
	
	/**
	 * @param handle
	 * here is where the entry into the nameNode conf is made
	 * Format: FileName: blockNumber, new proposed format
	 * FileName in a different file
	 * blocknumbers, new line separated in a different file
	 */

	public void removeFileHandleNew(int handle)
	{
		
		StringBuilder sb = new StringBuilder();
		/**
		 * File handle maps handle to a filename, the filename has to be appended in the NNConf
		 * the blocknumbers have to be written line separated into a new file
		 */
		String fileName  = fileHandletoFileName.get(handle);
		fileName += "\n";
		writeToConfNew(fileName);
		
		/**
		 * Create a new file, to store the block numbers in it
		 */
		
		for(int i=0;i<fileBlocks.get(handle).size();i++)
		{
			String block= fileBlocks.get(handle).get(i);
			
			sb.append(block.toString());
			if(i!=fileBlocks.get(handle).size()-1)
				sb.append("\n"); //write blocks line by line
			
		}
//		sb.append("\n"); not needed since, the content is written to a different file altogether, it
		//just introduces a blank line
		
		writeFileBlocks(sb.toString(),fileName);
		
		fileHandletoFileName.remove(handle);
		fileBlocks.remove(handle);
	}
	
	
	
	
	public void insertFileBlock(int handle,String numBlock)
	{
		fileBlocks.get(handle).add(numBlock);
	}
	
	
	public synchronized void writeToConf(String in)
	{
		PrintWriter pw;
		try {
			pw = new PrintWriter(new FileWriter(Constants.NAME_NODE_CONF,true));
		    pw.write(in);
	        pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
     
	}
	
	/**	 
	 * @param in
	 * Append the file name in the NNConfNew Structure
	 * Shesh
	 */
	public synchronized void writeToConfNew(String in)
	{
		PrintWriter pw;
		try {
			pw = new PrintWriter(new FileWriter(Constants.NAME_NODE_CONF_NEW,true));
		    pw.write(in);
		    pw.write("\n");
	        pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Shesh
	 * @param in
	 * @param fileName
	 */
	public void writeFileBlocks(String in,String fileName)
	{
		FileWriterClass myFileWriter = new FileWriterClass(Constants.PREFIX_DIR+fileName);
		myFileWriter.createFile();
		
		myFileWriter.writeonly(in);
		myFileWriter.closeFile();
	}

	
	
	
	/**
	 * returns hashmap for allblocks
	 * @param handle
	 * @param myHashMap
	 * @return
	 */
	public HashMap<String,Integer> removeFileHandleNew(int handle,HashMap<String,Integer> myHashMap)
	{
		StringBuilder sb = new StringBuilder();
		/**
		 * File handle maps handle to a filename, the filename has to be appended in the NNConf
		 * the block numbers have to be written line separated into a new file
		 */
		String fileName  = fileHandletoFileName.get(handle);
		
		writeToConfNew(fileName);
		
		/**
		 * Create a new file, to store the block numbers in it
		 */
		
		for(int i=0;i<fileBlocks.get(handle).size();i++)
		{
			String block= fileBlocks.get(handle).get(i);
			String[] blockWithVersion = block.split("\\.",0);
			System.out.println(blockWithVersion[0]+" all blocks format changed while we put");
			myHashMap.put(blockWithVersion[0] ,Integer.valueOf(blockWithVersion[1]));//this would add a block like 12.1 
			sb.append(block.toString());
			if(i!=fileBlocks.get(handle).size()-1)
				sb.append("\n"); //write blocks line by line
			
		}
		
		writeFileBlocks(sb.toString(),fileName);
		
		fileHandletoFileName.remove(handle);
		fileBlocks.remove(handle);
		
		return myHashMap;
	}
	
	/**
	 * Updates active block hashmap 
	 * @param handle
	 * @param myHashMap
	 * @return
	 */
	public HashMap<String,Integer> updateActiveBlocks(int handle,HashMap<String,Integer> myHashMap)
	{
		
		/**remove entries of all active file blocks **/
		for(int i=0;i<fileBlocks.get(handle).size();i++)
		{
			String block= fileBlocks.get(handle).get(i);
			String[] blockWithVersion = block.split("\\.",0);
			System.out.println(blockWithVersion[0]+" changed");
			myHashMap.remove(blockWithVersion[0]);//this would add a block like 12.1	
			
		}
				
		return myHashMap;
	}
	
}
