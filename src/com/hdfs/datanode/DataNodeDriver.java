package com.hdfs.datanode;

import static com.hdfs.miscl.Constants.DATA_NODE_ID;
import static com.hdfs.miscl.Constants.DATA_NODE_PORT;
import static com.hdfs.miscl.Constants.NAME_NODE;
import static com.hdfs.miscl.Constants.NAME_NODE_IP;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.nio.charset.StandardCharsets;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.ConnectException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

import javax.swing.plaf.FileChooserUI;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.BlockReportRequest;
import com.hdfs.miscl.Hdfs.BlockReportResponse;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.miscl.Hdfs.HeartBeatRequest;
import com.hdfs.miscl.Hdfs.HeartBeatResponse;
import com.hdfs.miscl.Hdfs.ReadBlockRequest;
import com.hdfs.miscl.Hdfs.ReadBlockResponse;
import com.hdfs.miscl.Hdfs.ReadBlockSizeRequest;
import com.hdfs.miscl.Hdfs.ReadBlockSizeResponse;
import com.hdfs.miscl.Hdfs.WriteBlockRequest;
import com.hdfs.miscl.Hdfs.WriteBlockResponse;
import com.hdfs.namenode.INameNode;
import com.hdfs.miscl.*;
import java.io.*;
import java.nio.*;

public class DataNodeDriver implements IDataNode {

	public static int id;
	public static int BINDING_PORT;
	
	public static List<String> dataBlocks;
	
	/**Interface methods start here **/
	public byte[] readBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
//		System.out.println("Hello");
		/**Here I need to 
		 * a) open the file
		 * b) Read the data
		 * c) convert into byte
		 * d) send back
		 */
		// add method in iDataNode
//		System.out.println("in reader");
		
		ReadBlockResponse.Builder readBlkResObj = ReadBlockResponse.newBuilder();
		
		ReadBlockRequest readBlkReqObj;
		try {
			readBlkReqObj = ReadBlockRequest.parseFrom(inp);
			String blockNumber = readBlkReqObj.getBlockNumber();
			

			
			File myFile = new File(getDirectoryName()+"/"+blockNumber);
			long FILESIZE = myFile.length();			
			int bytesToBeRead = (int) FILESIZE;
			
			byte[] newByteArray = null ;//= new byte[bytesToBeRead];
			Path path = Paths.get(getDirectoryName()+"/"+blockNumber);
			try {
				newByteArray = Files.readAllBytes(path);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//readBlkResObj.addData(ByteString.copyFrom(newByteArray));
			for(int i=0;i<FILESIZE;i++)
				readBlkResObj.addData(ByteString.copyFrom(newByteArray, i, 1));
			
			
//			System.out.println("done reading");
			readBlkResObj.setStatus(Constants.STATUS_SUCCESS);			
//			readBlkResObj.addData(ByteString.copyFrom(new String(newCharArray).getBytes()));
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			System.out.println("Invalid protobuf excpetion in ");
			readBlkResObj.setStatus(Constants.STATUS_FAILED);
		}
		
		return readBlkResObj.build().toByteArray();
		
	}


	public byte[] writeBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
//		System.out.println("In Method write Block");
//		byte[] receivedByteArray;
		
		WriteBlockResponse.Builder writeBlockRes = WriteBlockResponse.newBuilder();
		byte[] receivedByteArray = null; 
		
		try {
			final WriteBlockRequest writeBlockRequestObj = WriteBlockRequest.parseFrom(inp);
			receivedByteArray = new byte[writeBlockRequestObj.getDataList().size()];
//			System.out.print("received count"+writeBlockRequestObj.getDataCount());
			
			/**Received Byte array **/
			//receivedByteArray = writeBlockRequestObj.getData(0).toByteArray();
			
			/**Block locations object **/
			/** This is the needed code change **/
			for (int j = 0; j < writeBlockRequestObj.getDataList().size(); j++) {
				writeBlockRequestObj.getDataList().get(j).copyTo(receivedByteArray, j);
				//System.out.print((char)myBuffer[j]);
			}
			
			
//			System.out.print("wreiting length"+receivedByteArray.length);
			final BlockLocations blockLocObj = writeBlockRequestObj.getBlockInfo();
			
			final String blockNumber = blockLocObj.getBlockNumber();
			String str = new String(receivedByteArray, StandardCharsets.UTF_8);
			
			
			if(writeBlockRequestObj.getIsAppend())
			{

				String newBlock = writeBlockRequestObj.getNewBlockNum();
				
				if(!newBlock.equals("-1"))
				{
//					System.out.println("this just append");
//					System.out.println(str);
					
					createDuplicate(blockNumber,writeBlockRequestObj.getNewBlockNum(),receivedByteArray);
					
					int dataNodeCount = 1;
					
					if(blockLocObj.getLocationsCount() > 1)
						dataNodeCount = sendToNextDataNodeAppend(blockLocObj, blockNumber, writeBlockRequestObj);
					else
						dataNodeCount = writeBlockRequestObj.getCount()+1;
					
					
					writeBlockRes.setCount(dataNodeCount);
					writeBlockRes.setStatus(Constants.STATUS_SUCCESS);
					
					return writeBlockRes.build().toByteArray();
				}else
				{
		
					FileWriterClass fileWriterObj = new FileWriterClass(getDirectoryName()+"/"+blockNumber);
					fileWriterObj.createFile();
					//fileWriterObj.writeonly(str);
					/**
					 *  @Shweta
					 *  writing to file in bytes
					 */
					fileWriterObj.writeBytes(receivedByteArray);
					fileWriterObj.closeFile();
					
					/*update local list of blocks */
					insertBlockInDir(blockNumber);
					
					int dataNodeCount = 1;
					
					if(blockLocObj.getLocationsCount() > 1)
						dataNodeCount = sendToNextDataNode(blockLocObj, blockNumber, writeBlockRequestObj);
					else
						dataNodeCount = writeBlockRequestObj.getCount()+1;
					
					
					writeBlockRes.setCount(dataNodeCount);
					writeBlockRes.setStatus(Constants.STATUS_SUCCESS);
					
					return writeBlockRes.build().toByteArray();
				}
				
				
				
			}else
			{
				/**Write into FIle **/
				FileWriterClass fileWriterObj = new FileWriterClass(getDirectoryName()+"/"+blockNumber);
				fileWriterObj.createFile();
//				fileWriterObj.writeonly(str);
				fileWriterObj.writeBytes(receivedByteArray);
				fileWriterObj.closeFile();
				
				/*update local list of blocks */
				insertBlockInDir(blockNumber);
				
//				System.out.println("locations "+blockLocObj);
				
				
				/**This is the cascading part **/
				
				if(blockLocObj.getLocationsCount()>1)
				{
					
					 new Thread(new Runnable() {
			             @Override
			             public void run() {
			            	 try {
								sendToNextDataNode(blockLocObj,blockNumber,writeBlockRequestObj);
							} catch (RemoteException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
			             }
			         }).start();
				}
				
				writeBlockRes.setCount(3);
				writeBlockRes.setStatus(Constants.STATUS_SUCCESS);		
				return writeBlockRes.build().toByteArray();
				
			}
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

	private static void createDuplicate(String blockNumber, String newBlockNum,byte[] data) {
		// TODO Auto-generated method stub
		
		File inFile = new File(getDirectoryName()+"/"+blockNumber);
		File outFile = new File(getDirectoryName()+"/"+newBlockNum);
		
		
		try {
			Files.copy(inFile.toPath(), outFile.toPath());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		

		try {
						
			FileOutputStream fo = new FileOutputStream(outFile,true);
			fo.write(data);
			fo.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		dataBlocks.add(newBlockNum);
	}


	/**Interface methods end here 
	 * @param blockLocObj 
	 * @param blockNumber 
	 * @param writeBlockRequestObj 
	 * @throws RemoteException **/
	
	
	
	/* Returns count of datanodes that received append */
	public static int sendToNextDataNodeAppend(BlockLocations blockLocObj, String blockNumber, WriteBlockRequest writeBlockRequestObj) throws RemoteException
	{
		List<DataNodeLocation> locs = blockLocObj.getLocationsList();
		BlockLocations.Builder blkLocations = BlockLocations.newBuilder();
		blkLocations.setBlockNumber(blockNumber);
		
		//0 will be the current data node
		
		DataNodeLocation dataNode = locs.get(1); // this is the nexrt datanode in chain
				
		for(int i=1;i<locs.size();i++)
		{
				blkLocations.addLocations(locs.get(i));
		}
				
		

		try {
			Registry registry=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());
//			System.out.println("SEndign to "+dataNode.getPort());

			IDataNode dataStub;
			dataStub = (IDataNode) registry.lookup(Constants.DATA_NODE_ID);
			
			WriteBlockRequest.Builder req = WriteBlockRequest.newBuilder();
			req.setNewBlockNum(writeBlockRequestObj.getNewBlockNum());
			
			/** after new update **/
			for (int j = 0; j < writeBlockRequestObj.getDataList().size(); j++) {
				req.addData(writeBlockRequestObj.getDataList().get(j));
				//System.out.print((char)myBuffer[j]);
			}
			
//			req.addData(writeBlockRequestObj.getData(0));
			req.setBlockInfo(blkLocations);
			req.setIsAppend(true);
			req.setCount(writeBlockRequestObj.getCount()+1);
			
			byte[] data = dataStub.writeBlock(req.build().toByteArray());
			
			WriteBlockResponse res = WriteBlockResponse.parseFrom(data);
			
			return res.getCount();
			
			
			
			
		} catch (NotBoundException | InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		catch(ConnectException e)
		{
			System.out.println("Connect excpetion caught in Datanode driver class, sendtoNextnode method");
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Generic exception in Datanode driver class, sendtoNextnode method");
		}
		
		
		return writeBlockRequestObj.getCount()+1;
	}
	
	
	
	public static int sendToNextDataNode(BlockLocations blockLocObj, String blockNumber, WriteBlockRequest writeBlockRequestObj) throws RemoteException
	{
		List<DataNodeLocation> locs = blockLocObj.getLocationsList();
		BlockLocations.Builder blkLocations = BlockLocations.newBuilder();
		blkLocations.setBlockNumber(blockNumber);
		
//		System.out.println(locs);
		
		//0 will be the current data node
		
		DataNodeLocation dataNode = locs.get(1); // this is the nexrt datanode in chain
		
		for(int i=1;i<locs.size();i++)
		{
			blkLocations.addLocations(locs.get(i));
		}
		
//		blkLocations.addLocations(dataNode);
		
		
		try {
			
			Registry registry=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());
			System.out.println("SEndign to "+dataNode.getPort());


			IDataNode dataStub;
			
			dataStub = (IDataNode) registry.lookup(Constants.DATA_NODE_ID);
			
			WriteBlockRequest.Builder req = WriteBlockRequest.newBuilder();
			
			
			
			
			for(int j=0;j<writeBlockRequestObj.getDataCount();j++)
			{
				req.addData(writeBlockRequestObj.getData(j));	
			}
			
			
			
			
			
			req.setBlockInfo(blkLocations);
			req.setIsAppend(writeBlockRequestObj.getIsAppend());
			req.setNewBlockNum(writeBlockRequestObj.getNewBlockNum());
			req.setCount(writeBlockRequestObj.getCount()+1);
			
			byte[] data = dataStub.writeBlock(req.build().toByteArray());
			WriteBlockResponse res = WriteBlockResponse.parseFrom(data);
			
			return res.getCount();
			
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch(Exception e)
		{
			System.out.println("Generic exception in Datanode driver class, sendtoNextnode method");
		}
		return writeBlockRequestObj.getCount()+1;
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.setProperty("java.security.policy","./security.policy");

		
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		
		/**Need an argument from command line to uniquely identify the data Node **/	
		id = Integer.parseInt(args[0]);		
		System.out.println("Datanode "+id);

		
		createBlockDirectory();
		dataBlocks = readBlocksFromDir();
		
		bindToRegistry();
		
		sendBlockReport();
		
		sendHeartBeat();
		
		System.out.println(dataBlocks);
		
	}

	
	
	private static void bindToRegistry()
	{
		DataNodeDriver dataDriverObj = new DataNodeDriver();
		Registry register = null;
		
		BINDING_PORT = id+DATA_NODE_PORT;
		System.out.println("Binding port is "+BINDING_PORT);

		//Registering an object in java RMI environment
		try
		{
			System.out.println("IP is: "+getMyIP()+" Port is: "+BINDING_PORT);
			System.setProperty("java.rmi.server.hostname",getMyIP());
			register = LocateRegistry.createRegistry(BINDING_PORT);
			IDataNode dataStub = (IDataNode) UnicastRemoteObject.exportObject(dataDriverObj,BINDING_PORT);
			

			register.rebind(DATA_NODE_ID,dataStub);
		}
		catch(RemoteException e)
		{
			System.out.println("Remote Exception Caught DataNodeDriverClas  method:main");
		}
	}

	private static void blockReportRequest() {
		// TODO Auto-generated method stub
		try {
			Registry register = LocateRegistry.getRegistry(NAME_NODE_IP,Registry.REGISTRY_PORT);
			
			BlockReportRequest.Builder blockRepReqObj  = BlockReportRequest.newBuilder();
			
			
			/**Prepare data node location**/
			DataNodeLocation.Builder dataNodeLocObj = DataNodeLocation.newBuilder();
			dataNodeLocObj.setIp(getMyIP());
			dataNodeLocObj.setPort(BINDING_PORT);
			blockRepReqObj.setLocation(dataNodeLocObj);
			/**Set IP **/
			blockRepReqObj.setId(id);
			
			
			
			
			/**DOUBT Figure out what block locations to send **/


			
			blockRepReqObj.addAllBlockNumbers(dataBlocks);
			
			/**Create Stub to call name server methods **/
			INameNode nameNodeStub = (INameNode)register.lookup(NAME_NODE);
			byte[] result =  nameNodeStub.blockReport(blockRepReqObj.build().toByteArray());
			try {
				BlockReportResponse res = BlockReportResponse.parseFrom(result);
				
				if(res.getStatus(0)!=Constants.STATUS_SUCCESS)
				{
					System.out.println("Name node sent failure to Block Report");
					System.exit(0);
				}
				
				if(res.getDeleteBlocksCount()>0)
				{
					for(String block : res.getDeleteBlocksList())
					{
						System.out.println("Deleting "+block);
						removeBlockInDir(block);
					}
					
				}
				
				if(res.getBlockInfoCount()!=0)
				{
					for(int i=0;i<res.getBlockInfoCount();i++)
					{
						replicateBlock(res.getBlockInfo(i));
					}
				
				}
				
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
						
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void replicateBlock(BlockLocations blockLocations) {
		// TODO Auto-generated method stub
		
		String blockNumber = blockLocations.getBlockNumber();
		
		ReadBlockRequest.Builder readBlockReqObj = ReadBlockRequest.newBuilder();
		readBlockReqObj.setBlockNumber(blockNumber);
		System.out.println("Replicating "+blockNumber );
		
		/**Read block request call **/
		
		List<DataNodeLocation> dataNodes = blockLocations.getLocationsList();
		
		if(dataNodes==null || dataNodes.size()==0)
		{
			System.out.println("All nodes are down :( ");
			return;
		}
		
		int dataNodeCounter=0;
		
		DataNodeLocation thisDataNode = null;//dataNodes.get(dataNodeCounter);					
		String ip;// = thisDataNode.getIp();
		int port ; //= thisDataNode.getPort();
		
		
		IDataNode dataStub=null;
		
		boolean gotDataNodeFlag=false;
		
		/**
		 * The following do while loop tries to retrieve the data block that we are looking
		 * for, in case it is not present in one of the data locations that we first queried for.
		 * it goes to check with another data node that was sent in the response,
		 * this continues until we find the block or all data node locations exhaust
		 */
		do
		{
			try
			{
				thisDataNode = dataNodes.get(dataNodeCounter);
				ip = thisDataNode.getIp();
				port = thisDataNode.getPort();
											
				Registry registry2=LocateRegistry.getRegistry(ip,port);					
				dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
				gotDataNodeFlag=true;
			}
			catch (RemoteException e) {
				
				gotDataNodeFlag=false;
//				System.out.println("Remote Exception");
				dataNodeCounter++;
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}					
		while(gotDataNodeFlag==false && dataNodeCounter<dataNodes.size());
		
		/**This is an indication to say that even after checking all the datanodes
		 * for that particular block we couldn't get the block since all the nodes were down
		 * so we exit ( we may discuss and change it)
		 */
		if(dataNodeCounter == dataNodes.size())
		{
			System.out.println("All data nodes are down :( ");
			return;
		}
		
		
		byte[] responseArray;
		try {
			responseArray = dataStub.readBlock(readBlockReqObj.build().toByteArray());
			
			ReadBlockResponse readBlockResObj = ReadBlockResponse.parseFrom(responseArray);
			
			if(readBlockResObj.getStatus()==Constants.STATUS_FAILED)
			{
				System.out.println("In method openFileGet(), readError");
				return;
			}
			
			
			byte[] receivedByteArray = new byte[readBlockResObj.getDataList().size()];
			for (int j = 0; j < readBlockResObj.getDataList().size(); j++) {
				readBlockResObj.getDataList().get(j).copyTo(receivedByteArray, j);
			}
	
			String str = new String(receivedByteArray, StandardCharsets.UTF_8);		
			FileWriterClass fileWriteObj = new FileWriterClass(getDirectoryName()+"/"+blockNumber);
			fileWriteObj.createFile();
			fileWriteObj.writeBytes(receivedByteArray);
			fileWriteObj.closeFile();
			/*update local list of blocks */
			insertBlockInDir(blockNumber);
			
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}							

		
	}


	public static String getMyIP()
	{
		String myIp=null;
		Enumeration<NetworkInterface> n = null;
		try {
			n = NetworkInterface.getNetworkInterfaces();
			
		} catch (SocketException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	     for (; n.hasMoreElements();)
	     {
	             NetworkInterface e = n.nextElement();
//	             System.out.println("Interface: " + e.getName());
	             
	             
	             
	            	 Enumeration<InetAddress> a = e.getInetAddresses();
		             for (; a.hasMoreElements();)
		             {
		                     InetAddress addr = a.nextElement();
//		                     System.out.println("  " + addr.getHostAddress());
		                     if(e.getName().equals(Constants.CONNECTIVITY))
		                     {
		                    	myIp = addr.getHostAddress(); 
		                     }
		             }

	             
	            	 
	             
	             
	     }
	     
	     
	     
	     return myIp;

	}
	
	
	static void sendBlockReport()
	{
		 new Thread(new Runnable() {
             @Override
             public void run() {
            	 while(true)
            	 {
//            		 System.out.println("Sending blockReport");
            		 blockReportRequest();
            		
            		 try {
						Thread.sleep(Constants.BLOCK_REPORT_FREQ);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            		 
            	 }
            	 
             }
         }).start();
		
		
	}
	
	static void sendHeartBeat()
	{
		 new Thread(new Runnable() {
             @Override
             public void run() {
            	 while(true)
            	 {
//            		 System.out.println("Sending heartbeat");
            		 HeartBeatRequest.Builder req = HeartBeatRequest.newBuilder();
            		 req.setId(id);
            		 Registry register;
					try {
						register = LocateRegistry.getRegistry(NAME_NODE_IP,Registry.REGISTRY_PORT);
						 INameNode nameNodeStub = (INameNode)register.lookup(NAME_NODE);
		         		 byte[] result =  nameNodeStub.heartBeat(req.build().toByteArray());
		         		 
		         		 try {
							HeartBeatResponse res = HeartBeatResponse.parseFrom(result);
							if(res.getStatus()!=Constants.STATUS_SUCCESS)
							{
								System.out.println("Name node sent failure to heart beat");
								System.exit(0);
							}
							
						} catch (InvalidProtocolBufferException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}

		         		 
						
					} catch (RemoteException | NotBoundException e1) {
						// TODO Auto-generated catch block
						e1.printStackTrace();
					}
            		
            		 
            		 
            		 try {
						Thread.sleep(Constants.HEART_BEAT_FREQ);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
            	 }
            	 
             }
         }).start();
		
		
	}
	
	
	
	public static List<String> readBlocksFromFile()
	{
		ArrayList<String> blocks = new ArrayList<String>();
		
		BufferedReader buff;
		try {
			buff = new BufferedReader(new FileReader(Constants.DATA_NODE_CONF+id));
			String line=null;

			
			try {
				while((line = buff.readLine())!=null)
				{
					blocks.add(line);
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
//			e.printStackTrace();
			//create the file
			PrintWriter pw;
			try {
				pw = new PrintWriter(new FileWriter(Constants.DATA_NODE_CONF+id,true));
				pw.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			
		}
		
		
		
		return blocks;
	}
	
	public static List<String> readBlocksFromDir()
	{
		ArrayList<String> blocks = new ArrayList<String>();
		
		File folder = new File(getDirectoryName());
		File[] files = folder.listFiles();
		
		for(File file:files)
		{
			if(file.isFile())
			{
				if(!file.getName().matches(".*~$"))  //do not include temp files
					blocks.add(file.getName());
			}
		}
		
		return blocks;
	}
	
	
	public static synchronized void insertBlockInDir(String blockID)
	{
		dataBlocks.add(blockID);		
	}
	
	public static synchronized void removeBlockInDir(String blockID)
	{
		dataBlocks.remove(blockID);		
		File rmFile = new File(getDirectoryName()+"/"+blockID);
		rmFile.delete();
	}
	
	
	public static synchronized void insertBlock(String blockID)
	{
		dataBlocks.add(blockID);
		
		PrintWriter pw;
		try {
			pw = new PrintWriter(new FileWriter(Constants.DATA_NODE_CONF+id,true));
		    pw.write(blockID.toString());
		    pw.write("\n");
	        pw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	/* creates folder named DNConf_id which contains all blocks */ 
	public static void createBlockDirectory()
	{
		File dir = new File(getDirectoryName());
		
		if(!dir.exists())
		{
			dir.mkdir();
		}
	}
	
	public static String getDirectoryName()
	{
		return (Constants.DATA_NODE_CONF+id);
	}


	@Override
	public byte[] readBlockSize(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
		ReadBlockSizeResponse.Builder response = ReadBlockSizeResponse.newBuilder();
		response.setStatus(Constants.STATUS_NOT_FOUND);
		try {
			ReadBlockSizeRequest req = ReadBlockSizeRequest.parseFrom(inp);
			
			String fileName = getDirectoryName()+"/"+req.getBlockNumber();
			
			File file = new File(fileName);
			response.setSize(file.length());
			response.setStatus(Constants.STATUS_SUCCESS);
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	
		//return file.length();
		return response.build().toByteArray();
	}
	
}
