package com.hdfs.client;

import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;
import java.io.*;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.datanode.IDataNode;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.AssignBlockRequest;
import com.hdfs.miscl.Hdfs.AssignBlockResponse;
import com.hdfs.miscl.Hdfs.BlockLocationRequest;
import com.hdfs.miscl.Hdfs.BlockLocationResponse;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.CloseFileRequest;
import com.hdfs.miscl.Hdfs.CloseFileResponse;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.miscl.Hdfs.ListFilesRequest;
import com.hdfs.miscl.Hdfs.ListFilesResponse;
import com.hdfs.miscl.Hdfs.OpenFileRequest;
import com.hdfs.miscl.Hdfs.OpenFileResponse;
import com.hdfs.miscl.Hdfs.ReadBlockRequest;
import com.hdfs.miscl.Hdfs.ReadBlockResponse;
import com.hdfs.miscl.Hdfs.WriteBlockRequest;
import com.hdfs.namenode.INameNode;
import com.hdfs.datanode.*;
public class ClientDriver {

	public static String fileName;
	public static boolean getOrPutFlag;//true for read, false for write
	public static int fileHandle;
	public static byte[] byteArray;
	public static FileInputStream fis;
	public static long FILESIZE;
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		System.setProperty("java.security.policy","./security.policy");

		
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }

		
		fileName = args[0];
		
		/**args[1] can be get put or list **/
		if(args[1].toLowerCase().equals("put"))
		{		
			openFilePut();
		}
		else if(args[1].toLowerCase().equals("get"))
		{			
			openFileGet();
		}
		else if(args[1].toLowerCase().equals("list"))
		{
			//Calls the list method of the name node server
			callListBlocks();
		}
		
				
	}

	/**Call list blocks from Name node server**/
	public static void callListBlocks()
	{
		ListFilesRequest.Builder listFileReqObj = ListFilesRequest.newBuilder();
		listFileReqObj.setDirName("Shweta , Lamport and Berkeley, Sheshadri :)");		
		
		Registry registry;
		try {
			registry = LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			nameStub=(INameNode) registry.lookup(Constants.NAME_NODE);
			
			byte[] responseArray = nameStub.list(listFileReqObj.build().toByteArray());
			try {
				ListFilesResponse listFileResObj = ListFilesResponse.parseFrom(responseArray);
				List<String> fileNames = listFileResObj.getFileNamesList();
				
				System.out.println("File names are :\n");
				for(String name : fileNames)
				{
					System.out.println(name);
				}
				
				
			} catch (InvalidProtocolBufferException e) {
				 
				System.out.println("InvalidProtocolBufferException caught in callListBlocks: ClientDriverClass");
			}
			
		} catch (RemoteException | NotBoundException e) {
			
			System.out.println("Exception caught in callListBlocks: ClientDriverClass");
			
		}
		
		
	}
	
	
	/**Open file request method 
	 * Here the filename is obtained from the command line
	 * Along with that a flag is also passed telling whether it is a read request or a write request
	 * **/
	public static void openFileGet()
	{
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);		
		openFileReqObj.setForRead(true);
		
		
		FileWriterClass fileWriteObj = new FileWriterClass(Constants.OUTPUT_FILE+fileName);
		fileWriteObj.createFile();
		
		byte[] responseArray;
		
		try {
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			nameStub=(INameNode) registry.lookup(Constants.NAME_NODE);
			responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
			
			try {
				OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
				if(responseObj.getStatus()==Constants.STATUS_NOT_FOUND)
				{
					System.out.println("File not found fatal error");
					System.exit(0);
				}
				
				List<Integer> blockNums = responseObj.getBlockNumsList();
				BlockLocationRequest.Builder blockLocReqObj = BlockLocationRequest.newBuilder();
				
//				System.out.println(blockNums);
				/**Now perform Read Block Request  from all the blockNums**/
				blockLocReqObj.addAllBlockNums(blockNums);
												
				try {
					responseArray = nameStub.getBlockLocations(blockLocReqObj.build().toByteArray());
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				
				BlockLocationResponse blockLocResObj = BlockLocationResponse.parseFrom(responseArray);
//				System.out.println(blockLocResObj.toString());
				
				if(blockLocResObj.getStatus()==Constants.STATUS_FAILED)
				{
					System.out.println("Fatal error!");
					System.exit(0);
				}
				
				List<BlockLocations> blockLocations =  blockLocResObj.getBlockLocationsList();
				
				
				for(int i=0;i<blockLocations.size();i++)
				{
					BlockLocations thisBlock = blockLocations.get(i);
					
					int blockNumber = thisBlock.getBlockNumber();					
					List<DataNodeLocation> dataNodes = thisBlock.getLocationsList();
					
					if(dataNodes==null || dataNodes.size()==0)
					{
						System.out.println("All nodes are down :( ");
						System.exit(0);
					}
					
					int dataNodeCounter=0;
					
					DataNodeLocation thisDataNode = null;//dataNodes.get(dataNodeCounter);					
					String ip;// = thisDataNode.getIp();
					int port ; //= thisDataNode.getPort();
					
					
					IDataNode dataStub=null;
					
					boolean gotDataNodeFlag=false;
					
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
//							System.out.println("Remote Exception");
							dataNodeCounter++;
						} 
					}					
					while(gotDataNodeFlag==false && dataNodeCounter<dataNodes.size());
					
					if(dataNodeCounter == dataNodes.size())
					{
						System.out.println("All data nodes are down :( ");
						System.exit(0);
					}
					

					/**Construct Read block request **/
					ReadBlockRequest.Builder readBlockReqObj = ReadBlockRequest.newBuilder();
					readBlockReqObj.setBlockNumber(blockNumber);
					
					/**Read block request call **/
											
						responseArray = dataStub.readBlock(readBlockReqObj.build().toByteArray());
						ReadBlockResponse readBlockResObj = ReadBlockResponse.parseFrom(responseArray);
						
						if(readBlockResObj.getStatus()==Constants.STATUS_FAILED)
						{
							System.out.println("In method openFileGet(), readError");
							System.exit(0);
						}
						
						responseArray = readBlockResObj.getData(0).toByteArray();						
						String str = new String(responseArray, StandardCharsets.UTF_8);						
						fileWriteObj.writeonly(str);												

				}
				
				
			} catch (InvalidProtocolBufferException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (NotBoundException e) {
			System.out.println("Exception caught: NotBoundException ");			
		} catch (RemoteException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		fileWriteObj.closeFile();
		
	}
	
	
	/**Put Request from Client to name node **/
	public static void openFilePut()
	{
		
		byte[] responseArray;
		
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);
		openFileReqObj.setForRead(false);		
		
		try 
		{			
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			int status;
			
				try 
				{
					nameStub = (INameNode) registry.lookup(Constants.NAME_NODE);
					responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
					
					/**The response Array will contain the FileHandle status and the block numbers **/
					
					OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
					
					fileHandle = responseObj.getHandle();
					System.out.println("The file handle is "+fileHandle);
					
					status = responseObj.getStatus();
					if(status==Constants.STATUS_FAILED )//status failed change it
					{
						System.out.println("Fatal Error!");
						System.exit(0);
					}
					else if(status==Constants.STATUS_NOT_FOUND)
					{
						System.out.println("Duplicate File");
						System.exit(0);
					}
					
					AssignBlockRequest.Builder assgnBlockReqObj = AssignBlockRequest.newBuilder(); 
					
					
					/**required variables **/

					
					int offset=0;
					
					/**calculate block size **/
					int no_of_blocks=getNumberOfBlocks();					
					try {
						/**open the input stream **/
						fis = new FileInputStream(fileName);
					} catch (FileNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

//					System.out.println("No of blocks are "+no_of_blocks);
					if(no_of_blocks==0)
						no_of_blocks=1;
					
					/**FOR LOOP STARTS HERE **/
					for(int i=0;i<no_of_blocks;i++)
					{
						WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
						AssignBlockResponse assignResponseObj ;
						BlockLocations blkLocation ;
						List<DataNodeLocation> dataNodeLocations;
						DataNodeLocation dataNode;
						/**need to call assign block and write blocks **/
						
						assgnBlockReqObj.setHandle(fileHandle);
						
						/**Calling assign block **/
						responseArray = nameStub.assignBlock(assgnBlockReqObj.build().toByteArray());
						
						assignResponseObj = AssignBlockResponse.parseFrom(responseArray);
						
						status = assignResponseObj.getStatus();
						if(status==Constants.STATUS_FAILED)
						{
							System.out.println("Fatal Error!");
							System.exit(0);
						}
						
						blkLocation = assignResponseObj.getNewBlock();
						
						int blockNumber = blkLocation.getBlockNumber();
						System.out.println("Block number retured is "+blockNumber);
						
						dataNodeLocations = blkLocation.getLocationsList();
						
						dataNode = dataNodeLocations.get(0);
//						dataNodeLocations.remove(0);
						
						
						Registry registry2=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());

						System.out.println(dataNode);
						IDataNode dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
//						dataStub.readBlock(null);
						
//						System.out.println("Control enters here");
						/**read 32MB from file, send it as bytes, this fills in the byteArray**/
						
						byte[] byteArray = read32MBfromFile(offset);
						offset=offset+(int)Constants.BLOCK_SIZE;
						
						writeBlockObj.addData(ByteString.copyFrom(byteArray));
						writeBlockObj.setBlockInfo(blkLocation);
						
						dataStub.writeBlock(writeBlockObj.build().toByteArray());
												
					}
					
					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
					
					try {
						/**Close the input Stream **/
						fis.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
				catch (NotBoundException | InvalidProtocolBufferException e) {
					// TODO Auto-generated catch block
					System.out.println("Could not find NameNode");
					e.printStackTrace();
				}
				
			
		}catch (RemoteException e) {
			// TODO Auto-generated catch block
				e.printStackTrace();
		}		
		
	}

	
	public static int getNumberOfBlocks()
	{
		File inputFile = new File(fileName);
		if(!inputFile.exists())
		{
			System.out.println("File Does not exist");
			System.exit(0);
		}
		
		long fileSize = inputFile.length();
		FILESIZE=inputFile.length();
		double noOfBlocks = Math.ceil((double)fileSize*1.0/(double)Constants.BLOCK_SIZE*1.0);
		
//		System.out.println("The length of the file is "+fileSize+ " Number of blocks are "+(int)noOfBlocks);
		
		return (int)noOfBlocks;
	}
	
	/**Read 32MB size of data from the provided input file **/
	public static byte[] read32MBfromFile(int offset)
	{
		
		System.out.println("offset is "+offset);

		
		BufferedReader breader = null;
		try {
			breader = new BufferedReader(new FileReader(fileName) );
		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		

		
		
		int bytesToBeRead = (int)Constants.BLOCK_SIZE;
		
		int limit =offset+(int)Constants.BLOCK_SIZE; 
		
		if(limit >= (int) FILESIZE)
		{
			bytesToBeRead = (int)FILESIZE - offset;
		}
		else
		{
			bytesToBeRead = (int)Constants.BLOCK_SIZE;			
		}
		
		char[] newCharArray = new char[bytesToBeRead];
		
		try {
			breader.skip(offset);
			breader.read(newCharArray, 0, bytesToBeRead);
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
		try {
			breader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
//		System.out.println("The new char array is "+newCharArray.length);
		return new String(newCharArray).getBytes(StandardCharsets.UTF_8);
		
	}
	
	
	
	/**TEST CODE **/
	static void bindToRegistry()
	{
		
		try {
			
//			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
//			Registry registry=LocateRegistry.getRegistry("10.2.130.36",10001);
			Registry registry=LocateRegistry.getRegistry("10.2.129.126",10002);
			IDataNode stub;
			try {
				stub = (IDataNode) registry.lookup(Constants.DATA_NODE_ID);
				stub.readBlock(null);
			} catch (NotBoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Could not find NameNode");
				e.printStackTrace();
			}
			
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	

}
