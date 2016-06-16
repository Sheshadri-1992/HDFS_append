package com.hdfs.namenode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.BlockLocationRequest;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.DataNodeLocation;

public class GetFile {
	
	
	public GetFile() {
		// TODO Auto-generated constructor stub
	}
	
	Integer[] getFileDetails(String fileName)
	{
		BufferedReader buff;
		Integer [] blk_int =null;
		
		try {
			buff = new BufferedReader(new FileReader(Constants.NAME_NODE_CONF));
			String line=null;
			while((line = buff.readLine())!=null)
			{
				if(line.startsWith(fileName+":"))
				{
					break;
				}
			}
			
			if(line!=null)
			{
				String token[] = line.split(":");
				String blocks[] = token[1].split(",");
				blk_int = new Integer[blocks.length];
				for(int i=0;i<blocks.length;i++)
				{
					blk_int[i] = Integer.parseInt(blocks[i]);
				}
				
			}
			
			buff.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return blk_int;
		
	}

	public List<BlockLocations> getBlockLocations(List<Integer> blocks, HashMap<Integer, List<DataNodeLocation>> blockLocations) {
		// TODO Auto-generated method stub
		
		List<BlockLocations> resLocations = new ArrayList<>();
		
		for(Integer block : blocks)
		{
			BlockLocations.Builder blk = BlockLocations.newBuilder();
			blk.setBlockNumber(block);
			blk.addAllLocations(blockLocations.get(block));
			
			resLocations.add(blk.build());
			
		}
		
		return resLocations;
	}
	
	
	List<String> getAllFileNames()
	{
		BufferedReader buff;
	    List<String> fileNames = new ArrayList<>();
		
		try {
			buff = new BufferedReader(new FileReader(Constants.NAME_NODE_CONF));
			String line=null;
			while((line = buff.readLine())!=null)
			{
				String token[] = line.split(":");
				fileNames.add(token[0]);
			}	
			
			buff.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return fileNames;
		
	}

	

}
