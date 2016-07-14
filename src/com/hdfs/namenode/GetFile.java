package com.hdfs.namenode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;

import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.datanode.*;

public class GetFile {
	
	
	public GetFile() {
		// TODO Auto-generated constructor stub
	}
	/**this procedure has to be changed, currently file:<blocks> are being fetched 
	 * Now we just need to check for the filename and open another file for retrieving all data blocks
	 * @param fileName
	 * @return
	 */
	String[] getFileDetails(String fileName)
	{
		BufferedReader buff;
		String [] blk_int =null;
		
		try {
			buff = new BufferedReader(new FileReader(Constants.NAME_NODE_CONF_NEW));
			String line=null;
			while((line = buff.readLine())!=null)
			{
				if(line.startsWith(fileName))
				{
					break;
				}
			}
			
			if(line!=null)
			{
				/**open the file with the name**/
				int count = 0;
				Vector<String> myBlocks = new Vector<>();
				/**go inside the file directory**/
				FileReaderClass myFileReader = new FileReaderClass(Constants.PREFIX_DIR+fileName);
				myFileReader.openFile();
				
				line = myFileReader.buff_reader.readLine();
				while(line!=null)
				{
					count++;
					myBlocks.addElement(line);
					line = myFileReader.buff_reader.readLine();
				}
				myFileReader.closeFile();
				
				/**this is to copy all the contents in the vector into the string array **/
				blk_int = new String[myBlocks.size()];
				count = 0;
				for (String string : myBlocks) {
					String[] blockWithVersion = string.split("\\.");
					blk_int[count] = blockWithVersion[0]+"."+blockWithVersion[1];
					count++;
				}
				
				myBlocks.clear();
			}
			
			buff.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return blk_int;
		
	}

	public List<BlockLocations> getBlockLocations(List<String> blocks, HashMap<String, List<DataNodeLocation>> blockLocations) {
		// TODO Auto-generated method stub
		
		List<BlockLocations> resLocations = new ArrayList<>();
		
		for(String block : blocks)
		{
			BlockLocations.Builder blk = BlockLocations.newBuilder();
			blk.setBlockNumber(block);
			blk.addAllLocations(blockLocations.get(block));
			
			resLocations.add(blk.build());
			
		}
		
		return resLocations;
	}
	
	/**
	 * returns list of all filenames
	 * @return
	 */
	List<String> getAllFileNames()
	{
		BufferedReader buff;
	    List<String> fileNames = new ArrayList<>();
		
		try {
			buff = new BufferedReader(new FileReader(Constants.NAME_NODE_CONF_NEW)); //There is nothing to split :D 
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