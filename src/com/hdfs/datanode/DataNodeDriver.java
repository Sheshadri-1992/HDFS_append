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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

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
import com.hdfs.miscl.Hdfs.WriteBlockRequest;
import com.hdfs.namenode.INameNode;
import com.hdfs.miscl.*;
import java.io.*;
import java.nio.*;

public class DataNodeDriver implements IDataNode {

	public static int id;
	public static int BINDING_PORT;
	
	public static List<Integer> dataBlocks;
	
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
		
//		System.out.println("in reader");
		
		ReadBlockResponse.Builder readBlkResObj = ReadBlockResponse.newBuilder();
		
		ReadBlockRequest readBlkReqObj;
		try {
			readBlkReqObj = ReadBlockRequest.parseFrom(inp);
			int blockNumber = readBlkReqObj.getBlockNumber();
			

			
			File myFile = new File(blockNumber+"");
			long FILESIZE = myFile.length();			
			int bytesToBeRead = (int) FILESIZE;
			
			byte[] newByteArray = null ;//= new byte[bytesToBeRead];
			Path path = Paths.get(blockNumber+"");
			try {
				newByteArray = Files.readAllBytes(path);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			readBlkResObj.addData(ByteString.copyFrom(newByteArray));
//			for(int i=0;i<FILESIZE;i++)
//				readBlkResObj.addData(ByteString.copyFrom(newByteArray, i, 1));
			
			
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
		byte[] receivedByteArray;
		
		try {
			final WriteBlockRequest writeBlockRequestObj = WriteBlockRequest.parseFrom(inp);
			/**Received Byte array **/
			receivedByteArray = writeBlockRequestObj.getData(0).toByteArray();
			/**Block locations object **/
			final BlockLocations blockLocObj = writeBlockRequestObj.getBlockInfo();
			
			final int blockNumber = blockLocObj.getBlockNumber();
			
			
			String str = new String(receivedByteArray, StandardCharsets.UTF_8);
			
			/**Write into FIle **/
			FileWriterClass fileWriterObj = new FileWriterClass(blockNumber+"");
			fileWriterObj.createFile();
			fileWriterObj.writeonly(str);
			fileWriterObj.closeFile();
			
			/*update local list of blocks */
			insertBlock(blockNumber);
			
//			System.out.println("locations "+blockLocObj);
			
			
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
			
			
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	/**Interface methods end here 
	 * @param blockLocObj 
	 * @param blockNumber 
	 * @param writeBlockRequestObj 
	 * @throws RemoteException **/
	
	
	public static void sendToNextDataNode(BlockLocations blockLocObj, int blockNumber, WriteBlockRequest writeBlockRequestObj) throws RemoteException
	{
		List<DataNodeLocation> locs = blockLocObj.getLocationsList();
		BlockLocations.Builder blkLocations = BlockLocations.newBuilder();
		blkLocations.setBlockNumber(blockNumber);
		
		DataNodeLocation dataNode = locs.get(1);
		
		
		blkLocations.addLocations(dataNode);
		
		Registry registry=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());

		IDataNode dataStub;
		try {
			dataStub = (IDataNode) registry.lookup(Constants.DATA_NODE_ID);
			
			WriteBlockRequest.Builder req = WriteBlockRequest.newBuilder();
			
			req.addData(writeBlockRequestObj.getData(0));
			req.setBlockInfo(blkLocations);
			
			dataStub.writeBlock(req.build().toByteArray());
			
			
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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

		
		dataBlocks = readBlocksFromFile();
//		System.out.println(dataBlocks);
		
		bindToRegistry();
		
		sendBlockReport();
		
		sendHeartBeat();
		
		
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
				
				
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
						
		} catch (RemoteException | NotBoundException e) {
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
	
	public static List<Integer> readBlocksFromFile()
	{
		ArrayList<Integer> blocks = new ArrayList<Integer>();
		
		BufferedReader buff;
		try {
			buff = new BufferedReader(new FileReader(Constants.DATA_NODE_CONF+id));
			String line=null;

			
			try {
				while((line = buff.readLine())!=null)
				{
					blocks.add(Integer.parseInt(line));
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
	
	public static synchronized void insertBlock(Integer blockID)
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
	
}
