package com.hdfs.namenode;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Vector;

import com.hdfs.miscl.Constants;

public class PutFile {

	private HashMap<Integer,String> fileHandle;   // fileHandle ,fileName 
	private HashMap<Integer,Vector<Integer>> fileBlocks;   // fileHandle , blocksAssigned so far
	
	public PutFile() {
		// TODO Auto-generated constructor stub
		fileHandle = new HashMap<>();
		fileBlocks = new HashMap<>();
		
	}
	
	public void insertFileHandle(String fileName,int handle)
	{
		fileHandle.put(handle,fileName);
		fileBlocks.put(handle,new Vector<Integer>());
	}
	
	public void removeFileHandle(int handle)
	{
		
		StringBuilder sb = new StringBuilder();
		sb.append(fileHandle.get(handle));
		sb.append(":");
		for(int i=0;i<fileBlocks.get(handle).size();i++)
		{
			Integer block= fileBlocks.get(handle).get(i);
			
			sb.append(block.toString());
			if(i!=fileBlocks.get(handle).size()-1)
				sb.append(",");
			
		}
		sb.append("\n");
		
		writeToConf(sb.toString());
		
		fileHandle.remove(handle);
		fileBlocks.remove(handle);
	}
	
	public void insertFileBlock(int handle,int numBlock)
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
	
}
