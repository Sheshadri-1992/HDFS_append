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
import com.hdfs.miscl.Hdfs.WriteBlockResponse;
import com.hdfs.namenode.INameNode;
import com.hdfs.datanode.*;
public class ClientDriver {

	public static String fileName;
	public static boolean getOrPutFlag;//true for read, false for write
	public static int fileHandle;
	public static byte[] byteArray;
	public static FileInputStream fis;
	public static long FILESIZE;
	public static String appendFile;
	
	
	
	
	public static void main(String[] args) throws NotBoundException, IOException {
		// TODO Auto-generated method stub

		
		System.setProperty("java.security.policy","./security.policy");

		
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }

		
		fileName = args[0];
		appendFile="test";
		
		
		
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
		else if(args[1].toLowerCase().equals("append"))
		{		
//			openFileAppend();
			
			appendFile = args[2];
			System.out.println("Append "+appendFile);
			appendTesting();
		}
		
				
	}
	
	public static void appendTesting()
	{
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);
		openFileReqObj.setForRead(false);
		openFileReqObj.setIsAppend(true);
		
		
		try {
			Registry registry = LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			int status;
			nameStub = (INameNode) registry.lookup(Constants.NAME_NODE);
			byte[] responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
			
			/**The response Array will contain the FileHandle status and the block numbers **/
			
			OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
			fileHandle = responseObj.getHandle();
			System.out.println("The file handle is "+fileHandle);
			
			if(responseObj.getStatus()==Constants.STATUS_NOT_FOUND||responseObj.getStatus()==Constants.STATUS_FAILED)
			{
				System.out.println("fatal error");
				System.exit(0);
			}
			
			List<String> blockNums = responseObj.getBlockNumsList();
			String last_blocknum=blockNums.get(0);
			String newBlockNum = responseObj.getNewBlockNum();		
			
			int size=(int) responseObj.getSize();
			System.out.println("size of the file is "+size);
			/**remaining size **/
			int remainSize=(Constants.BLOCK_SIZE)-size;//1,000,000 - 841
			
			System.out.println("The remaining size  is "+remainSize);
		    BufferedReader breader = null;
		    breader = new BufferedReader(new FileReader(appendFile) );
		    File myFile = new File(appendFile);
		    int bytesToRead = 0;
		    if(myFile.exists())
		    {
		      bytesToRead = (int)myFile.length();
		    }
		    
		    /*Handles all cases */
		    sendFileAsAppendCaseThree(responseObj,remainSize,bytesToRead);
		    
		    breader.close();
		      
			
			
		} catch (RemoteException | NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	/**
	 * bytes to read are more than the available free space in the last block
	 * so new append blocks have to be sent
	 * 1. append last block
	 * 2. ask for new assign blocks
	 */
	private static void sendFileAsAppendCaseThree(OpenFileResponse resObj,int remainSize,int totalBytes) {
		// TODO Auto-generated method stub
		/**
		 * 1. prepare a write block request, for that get block locations 
		 */
		
		
		byte[] firstData;

		/**if totalbytes is less than remaining then we only need to append
		 * if totalbytes is greater then append and and create new block
		 */
		INameNode nameStub=null;
		
		boolean isException = false;
		
		if(totalBytes<remainSize)
		{
			firstData = new byte[totalBytes];
			remainSize=totalBytes;
		}else
		{
			firstData = new byte[remainSize];
		}

		
		try
		{
			
			InputStream in = new FileInputStream(appendFile);
			Registry registry = LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			nameStub = (INameNode) registry.lookup(Constants.NAME_NODE);

			WriteBlockRequest.Builder writeReqObj = WriteBlockRequest.newBuilder();


			//block locations object need to send request on the new block
			BlockLocationRequest.Builder blockReqObj = BlockLocationRequest.newBuilder();
			List<String> myBlocks = new ArrayList<String>();
			myBlocks.add(resObj.getBlockNums(0));
			blockReqObj.addAllBlockNums(myBlocks);

			byte[] responseArray = nameStub.getBlockLocations(blockReqObj.build().toByteArray());

			BlockLocationResponse myResponse = BlockLocationResponse.parseFrom(responseArray);
			if(myResponse.getStatus()==Constants.STATUS_FAILED)
			{
				System.out.print("block response not got bye bye");
				System.exit(0);				
			}

			BlockLocations lastBlock = myResponse.getBlockLocations(0);

			//read data of size remain size

			in.read(firstData, 0, remainSize);

			writeReqObj.setBlockInfo(lastBlock); // added the details of last blocks
			writeReqObj.setIsAppend(true);
			writeReqObj.setNewBlockNum(resObj.getNewBlockNum());
			writeReqObj.setCount(0);
			writeReqObj.addData(ByteString.copyFrom(firstData));// initial set of data is sent


			List<BlockLocations> blockLocations =  myResponse.getBlockLocationsList();				
			BlockLocations thisBlock = blockLocations.get(0); //get the location of the block that we are about to append			
			List<DataNodeLocation> dataNodes = thisBlock.getLocationsList();//location of all data nodes that contain this block

			int dataNodeCounter=0;			
			DataNodeLocation thisDataNode = null;				
			String ip;
			int port ; 
			
			IDataNode dataStub=null;


			thisDataNode = dataNodes.get(dataNodeCounter);
			ip = thisDataNode.getIp();
			port = thisDataNode.getPort();

			Registry registry2=LocateRegistry.getRegistry(ip,port);					
			dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);

			byte[] writeReqResponse = dataStub.writeBlock(writeReqObj.build().toByteArray());
			WriteBlockResponse writeBlkRes = WriteBlockResponse.parseFrom(writeReqResponse);
			if(writeBlkRes.getStatus()!=Constants.STATUS_FAILED)
			{
				if(writeBlkRes.getCount()>=2)
				{
					System.out.println("Okay! first phase of append done, now its normal writes");
				}
				else
				{
					System.out.println("Aborting due to lack of votes");
					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setDecision(0);//abort
					closeFileObj.setHandle(resObj.getHandle()); //handle
					nameStub.closeFile(closeFileObj.build().toByteArray());
				}
			}


			int amountBytesRemaining = totalBytes - remainSize;

			boolean success = true;
			
			if(amountBytesRemaining>0)
			{
				success = sendRemainingBytesAppend(resObj,amountBytesRemaining,nameStub,in);
			}

			in.close();//close the file
			
			

			CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
			if(success)
				closeFileObj.setDecision(1);//commit
			else
				closeFileObj.setDecision(0);//abort
				
			
			closeFileObj.setHandle(resObj.getHandle()); //handle
			nameStub.closeFile(closeFileObj.build().toByteArray());
		}
		catch(Exception e)
		{
			e.printStackTrace();
			System.out.println("Whats up exception caught!");
			isException=true;
		}
		
		if(isException)
		{
			CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
			closeFileObj.setDecision(0);//abort
			closeFileObj.setHandle(resObj.getHandle()); //handle
			try {
				nameStub.closeFile(closeFileObj.build().toByteArray());
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
				
	}

	

	/**method to send remaining bytes as one by one in blocks
	 * @return **/
	private static boolean sendRemainingBytesAppend(OpenFileResponse resObj, int amountBytesRemaining, INameNode nameStub,
			InputStream in) {
		// TODO Auto-generated method stub
	
		System.out.println("Sending new block");
		
		while(amountBytesRemaining>0)
		{
			AssignBlockRequest.Builder assgnBlk = AssignBlockRequest.newBuilder();
			assgnBlk.setHandle(resObj.getHandle());
			assgnBlk.setIsAppend(true); 
			
			byte[] assignResponse;
			try {
				
				assignResponse = nameStub.assignBlock(assgnBlk.build().toByteArray());
				AssignBlockResponse assgnResponse = AssignBlockResponse.parseFrom(assignResponse);	
				
				if(assgnResponse.getStatus()!=Constants.STATUS_FAILED)
				{
					/** we get the new block number **/
					System.out.println("new block returned from NN");
				}
				
				BlockLocations thisBlock =  assgnResponse.getNewBlock();			
				 //get the location of the block that we are about to append			
				List<DataNodeLocation> dataNodes = thisBlock.getLocationsList();//location of all data nodes that contain this block
				
				int dataNodeCounter=0;			
				DataNodeLocation thisDataNode = null;				
				String ip;
				int port ; 
				
				IDataNode dataStub=null;
				
				thisDataNode = dataNodes.get(dataNodeCounter);
				ip = thisDataNode.getIp();
				port = thisDataNode.getPort();
											
				Registry registry2=LocateRegistry.getRegistry(ip,port);					
				dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
				
				
				int sendBytes = 0;
				if(amountBytesRemaining<Constants.BLOCK_SIZE)
				{
					sendBytes = amountBytesRemaining;
					amountBytesRemaining = 0;
				}
				else
				{
					sendBytes = Constants.BLOCK_SIZE;
					amountBytesRemaining = amountBytesRemaining - Constants.BLOCK_SIZE;
				}
				
				byte[] data = new byte[sendBytes];
				
				in.read(data, 0, sendBytes);
				
				WriteBlockRequest.Builder writeBlkReq = WriteBlockRequest.newBuilder();
				writeBlkReq.setCount(0);
				writeBlkReq.setIsAppend(true);
				writeBlkReq.setNewBlockNum("-1");
				writeBlkReq.setBlockInfo(thisBlock);
				writeBlkReq.addData(ByteString.copyFrom(data));
				
				byte[] writeReqResponse = dataStub.writeBlock(writeBlkReq.build().toByteArray());
				WriteBlockResponse writeBlkRes = WriteBlockResponse.parseFrom(writeReqResponse);
				
				if(writeBlkRes.getStatus()!=Constants.STATUS_FAILED)
				{
					if(writeBlkRes.getCount()>=2)
					{
						System.out.println("Second Phase New Block Success");
					}
					else
					{
						return false;
					}
				}
				
				
			//	skip = skip + sendBytes;
			} catch (IOException | NotBoundException e) {
				// TODO Auto-generated catch block
				System.out.println("exception in send remaining bytes");
				e.printStackTrace();
				return false;
			} 
		}
		
		return true;
		
	}
	

	private static void openFileAppend() throws NotBoundException, IOException {
		byte[] responseArray;
		OpenFileRequest.Builder openFileReqObj = OpenFileRequest.newBuilder();
		openFileReqObj.setFileName(fileName);
		openFileReqObj.setForRead(false);
		openFileReqObj.setIsAppend(true);
		
		
		Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
		INameNode nameStub;
		int status;
		nameStub = (INameNode) registry.lookup(Constants.NAME_NODE);
		responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
		
		/**The response Array will contain the FileHandle status and the block numbers **/
		
		OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
		fileHandle = responseObj.getHandle();
		System.out.println("The file handle is "+fileHandle);
		
		if(responseObj.getStatus()==Constants.STATUS_NOT_FOUND||responseObj.getStatus()==Constants.STATUS_FAILED)
		{
			System.out.println("fatal error");
			System.exit(0);
		}
		
		
		List<String> blockNums = responseObj.getBlockNumsList();
		String last_blocknum=blockNums.get(0);
		String newBlockNum = responseObj.getNewBlockNum();		
		
		int size=(int) responseObj.getSize();
		System.out.println("size of the file is "+size);
		//assuming size came in bytes
		int remainsize=(Constants.BLOCK_SIZE)-size;//1,000,000 - 841
		//
		//WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
		
//		    String newLine = System.getProperty("line.separator");
//		    BufferedWriter writer = null;
//		    //System.out.println("Reading Strings from console");
//		    
//		    writer = new BufferedWriter( new FileWriter( "test.txt"));
//		   
//		    
//		      //You use System.in to get the Strings entered in console by user
//		      try
//		      {
//		      //You need to create BufferedReader which has System.in to get user input
//		      BufferedReader br = new BufferedReader(new
//		                              InputStreamReader(System.in));
//		      String userInput="";
//		      System.out.println("Enter text to append...");
//		      System.out.println("Enter 'quit' to quit.");
//		      do{
//		        
//		          userInput = (String) br.readLine();
//		         if( userInput.equals("quit"))
//		        	 break;
//		          writer.write(  userInput);
////		          writer.write("\n");
//		        } while(!userInput.equals("quit"));
//		      
//		      if ( writer != null)
//		          writer.close( );
//		      }
//		      catch(Exception e)
//		      {
//		      }
		      System.out.println("The remaining size  is "+remainsize);
		      BufferedReader breader = null;
		      breader = new BufferedReader(new FileReader(appendFile) );
		      File myFile = new File(appendFile);
		      int bytesToRead = 0;
		      if(myFile.exists())
		      {
		    	  bytesToRead = (int)myFile.length();
		      }
		      
		      //case 0: where the remain size of the last block is 0;
		      if(remainsize==0)
		      {
		    	  double noOfBlocks = Math.ceil((double)bytesToRead*1.0/(double)Constants.BLOCK_SIZE*1.0);
	    		  if(noOfBlocks==0)
	    			  noOfBlocks=1;
	    		  AssignBlockRequest.Builder assgnBlockReqObj = AssignBlockRequest.newBuilder();
	    		  FILESIZE= (int)myFile.length();
	    		  int offset=0;
					
	    		  for(int i=0;i<noOfBlocks;i++)
					{
	    			  WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
	    			  AssignBlockResponse assignResponseObj ;
						BlockLocations blkLocation ;
						List<DataNodeLocation> dataNodeLocations;
						DataNodeLocation dataNode;
						/**need to call assign block and write blocks **/
						
						assgnBlockReqObj.setHandle(fileHandle);
						assgnBlockReqObj.setIsAppend(true); //to indicate that it is append
						
						responseArray = nameStub.assignBlock(assgnBlockReqObj.build().toByteArray());
                        assignResponseObj = AssignBlockResponse.parseFrom(responseArray);
						
						status = assignResponseObj.getStatus();
						if(status==Constants.STATUS_FAILED)
						{
							System.out.println("Fatal Error!");
							System.exit(0);
						}
						
						blkLocation = assignResponseObj.getNewBlock();
						
						String blockNumber = blkLocation.getBlockNumber();
						System.out.println("Block number retured is "+blockNumber);
                        dataNodeLocations = blkLocation.getLocationsList();
						
						dataNode = dataNodeLocations.get(0);
						Registry registry2=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());

						System.out.println(dataNode);
						IDataNode dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
						byte[] byteArray = read32MBfromFile(offset);
						offset=offset+(int)Constants.BLOCK_SIZE;
						
						writeBlockObj.addData(ByteString.copyFrom(byteArray));
						writeBlockObj.setBlockInfo(blkLocation);
						writeBlockObj.setIsAppend(false);
						writeBlockObj.setCount(0);
						writeBlockObj.setNewBlockNum("-1");
						
						dataStub.writeBlock(writeBlockObj.build().toByteArray());
						
						byte[] response_write=dataStub.writeBlock(writeBlockObj.build().toByteArray());
						
						WriteBlockResponse reswriteobj= WriteBlockResponse.parseFrom(response_write);
						int sucess_count=reswriteobj.getCount();
						if(sucess_count>=2)
						{
							System.out.println("sucess in append");

							CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
							closeFileObj.setHandle(fileHandle);
							closeFileObj.setDecision(1);
							
							byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
							CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
							if(closeResObj.getStatus()==Constants.STATUS_FAILED)
							{
								System.out.println("Close File response Status Failed");
								System.exit(0);
							}
							//comment
							//comment
						}
						else
						{
							CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
							closeFileObj.setHandle(fileHandle);
							closeFileObj.setDecision(0);
							
							byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
							CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
							if(closeResObj.getStatus()==Constants.STATUS_FAILED)
							{
								System.out.println("Close File response Status Failed");
								System.exit(0);
							}
							
						}

						
					}
	    			  
	    			  
	    		  
		      }
		      
		      
		     //case 1 where there is a chance of multiple blocks
		      if((remainsize!=0)&&(remainsize<bytesToRead))
		      {
		    	  char[] newCharArray = new char[bytesToRead];
			      breader.read(newCharArray, 0, bytesToRead);
			      
			      byte[] array=new String(newCharArray).getBytes(StandardCharsets.UTF_8);
			      BlockLocationRequest.Builder blockLocReqObj = BlockLocationRequest.newBuilder();
					
					
					
				blockLocReqObj.addAllBlockNums(blockNums);
												
				try {
					responseArray = nameStub.getBlockLocations(blockLocReqObj.build().toByteArray());
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				
				BlockLocationResponse blockLocResObj = BlockLocationResponse.parseFrom(responseArray);
			
				if(blockLocResObj.getStatus()==Constants.STATUS_FAILED)
				{
					System.out.println("Fatal error!");
					System.exit(0);
				}
				
				
				List<BlockLocations> blockLocations =  blockLocResObj.getBlockLocationsList();
				
				BlockLocations thisBlock = blockLocations.get(0);
				String blockNumber = thisBlock.getBlockNumber(); // this is the new block number
				List<DataNodeLocation> dataNodes = thisBlock.getLocationsList();
				
		      
				int dataNodeCounter=0;
				
				DataNodeLocation thisDataNode = null;					
				String ip;
				int port ; 
				
				
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
				WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
				writeBlockObj.addData(ByteString.copyFrom(array));
				writeBlockObj.setBlockInfo(thisBlock);
				writeBlockObj.setIsAppend(true);
				writeBlockObj.setCount(0);
				// need to set this as null, because the subsequent  calls to assign block can be
				// handled with the new block number already present in the set block info
				// however, the writing of these new append blocks should be synchronous
//				writeBlockObj.setNewBlockNum(null);
				
				byte[] response_write=dataStub.writeBlock(writeBlockObj.build().toByteArray());
				
				WriteBlockResponse reswriteobj= WriteBlockResponse.parseFrom(response_write);
				int sucess_count=reswriteobj.getCount();
				if(sucess_count>=2)
				{
					System.out.println("Shouldn't call this right away");

//					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
//					closeFileObj.setHandle(fileHandle);
//					closeFileObj.setDecision(1);
					
//					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
//					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
//					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
//					{
//						System.out.println("Close File response Status Failed");
//						System.exit(0);
//					}
					//comment
					//comment
				}
				else
				{
					System.out.println("Append did not get enough votes");
					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					closeFileObj.setDecision(0);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
					
				}
				
				
				 double noOfBlocks = Math.ceil((double)(bytesToRead-remainsize)*1.0/(double)Constants.BLOCK_SIZE*1.0);
	    		  if(noOfBlocks==0)
	    			  noOfBlocks=1;
	    		  AssignBlockRequest.Builder assgnBlockReqObj = AssignBlockRequest.newBuilder();
	    		  FILESIZE= (int)myFile.length();
	    		  int offset=remainsize;
					
	    		  for(int i=0;i<noOfBlocks;i++)
					{
		    			writeBlockObj = WriteBlockRequest.newBuilder();
		    			AssignBlockResponse assignResponseObj ;
						BlockLocations blkLocation ;
						List<DataNodeLocation> dataNodeLocations;
						DataNodeLocation dataNode;
						/**need to call assign block and write blocks **/
						
						assgnBlockReqObj.setHandle(fileHandle);
						assgnBlockReqObj.setIsAppend(true);
						
						responseArray = nameStub.assignBlock(assgnBlockReqObj.build().toByteArray());
                         assignResponseObj = AssignBlockResponse.parseFrom(responseArray);
						
						status = assignResponseObj.getStatus();
						if(status==Constants.STATUS_FAILED)
						{
							System.out.println("Fatal Error!");
							System.exit(0);
						}
						
						blkLocation = assignResponseObj.getNewBlock();
						
						blockNumber = blkLocation.getBlockNumber();
						System.out.println("Block number retured is "+blockNumber);
                       dataNodeLocations = blkLocation.getLocationsList();
						
						dataNode = dataNodeLocations.get(0);
						Registry registry2=LocateRegistry.getRegistry(dataNode.getIp(),dataNode.getPort());

						System.out.println(dataNode);
					        dataStub = (IDataNode) registry2.lookup(Constants.DATA_NODE_ID);
						byte[] byteArray = read32MBfromFile(offset);
						offset=offset+(int)Constants.BLOCK_SIZE;
						
						writeBlockObj.addData(ByteString.copyFrom(byteArray));
						writeBlockObj.setBlockInfo(blkLocation);
						writeBlockObj.setIsAppend(false);
						writeBlockObj.setCount(0);
						writeBlockObj.setNewBlockNum(null);
						
						dataStub.writeBlock(writeBlockObj.build().toByteArray());
						
						response_write=dataStub.writeBlock(writeBlockObj.build().toByteArray());
						
						 reswriteobj= WriteBlockResponse.parseFrom(response_write);
					      sucess_count=reswriteobj.getCount();
						if(sucess_count>=2)
						{
							System.out.println("again shouldn't call this right away");

//							CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
//							closeFileObj.setHandle(fileHandle);
//							closeFileObj.setDecision(1);
//							
//							byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
//							CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
//							if(closeResObj.getStatus()==Constants.STATUS_FAILED)
//							{
//								System.out.println("Close File response Status Failed");
//								System.exit(0);
//							}
							//comment
							//comment
						}
						else
						{
							CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
							closeFileObj.setHandle(fileHandle);
							closeFileObj.setDecision(0);
							
							byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
							CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
							if(closeResObj.getStatus()==Constants.STATUS_FAILED)
							{
								System.out.println("Close File response Status Failed");
								System.exit(0);
							}
							
						}

						
					}
		    	  
	    		   System.out.println("Now we can have append");
	    		  
	    		    CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					closeFileObj.setDecision(1);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
		    	  
		      }
		      
		      
		      
		      
		      
		      
		      if((remainsize!=0)&&(remainsize>bytesToRead))
		      {
		      char[] newCharArray = new char[bytesToRead];
		      breader.read(newCharArray, 0, bytesToRead);
		      
		      byte[] array=new String(newCharArray).getBytes(StandardCharsets.UTF_8);
		      
		      
		      BlockLocationRequest.Builder blockLocReqObj = BlockLocationRequest.newBuilder();
				
				
				
				blockLocReqObj.addAllBlockNums(blockNums);
												
				try {
					responseArray = nameStub.getBlockLocations(blockLocReqObj.build().toByteArray());
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				
				BlockLocationResponse blockLocResObj = BlockLocationResponse.parseFrom(responseArray);
			
				if(blockLocResObj.getStatus()==Constants.STATUS_FAILED)
				{
					System.out.println("Fatal error!");
					System.exit(0);
				}
				
				
				List<BlockLocations> blockLocations =  blockLocResObj.getBlockLocationsList();
				
				BlockLocations thisBlock = blockLocations.get(0);
				String blockNumber = thisBlock.getBlockNumber();
				List<DataNodeLocation> dataNodes = thisBlock.getLocationsList();
				
		      
				int dataNodeCounter=0;
				
				DataNodeLocation thisDataNode = null;					
				String ip;
				int port ; 
				
				
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
//						System.out.println("Remote Exception");
						dataNodeCounter++;
					} 
				}					
				while(gotDataNodeFlag==false && dataNodeCounter<dataNodes.size());
				
				
				if(dataNodeCounter == dataNodes.size())
				{
					System.out.println("All data nodes are down :( ");
					System.exit(0);
				}
				WriteBlockRequest.Builder writeBlockObj = WriteBlockRequest.newBuilder();
				writeBlockObj.addData(ByteString.copyFrom(array));
				writeBlockObj.setBlockInfo(thisBlock);
				writeBlockObj.setIsAppend(true);
				writeBlockObj.setCount(0);
				writeBlockObj.setNewBlockNum(newBlockNum);
				byte[] response_write=dataStub.writeBlock(writeBlockObj.build().toByteArray());
				
				WriteBlockResponse reswriteobj= WriteBlockResponse.parseFrom(response_write);
				int sucess_count=reswriteobj.getCount();
				if(sucess_count>=2)
				{
					System.out.println("sucess in append");

					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					closeFileObj.setDecision(1);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
					//comment
					//comment
				}
				else
				{
					CloseFileRequest.Builder closeFileObj = CloseFileRequest.newBuilder();
					closeFileObj.setHandle(fileHandle);
					closeFileObj.setDecision(0);
					
					byte[] receivedArray = nameStub.closeFile(closeFileObj.build().toByteArray());
					CloseFileResponse closeResObj = CloseFileResponse.parseFrom(receivedArray);
					if(closeResObj.getStatus()==Constants.STATUS_FAILED)
					{
						System.out.println("Close File response Status Failed");
						System.exit(0);
					}
					
				}
		      }
				
		      
		  }
		
		
		
	

	/**Call list blocks from Name node server**/
	public static void callListBlocks()
	{
		ListFilesRequest.Builder listFileReqObj = ListFilesRequest.newBuilder();
		
		if(fileName.equals("."))
		{
			listFileReqObj.setDirName("");
		}else
		{
			listFileReqObj.setDirName(fileName);
		}
		
		System.out.println(" helllo"+listFileReqObj);
				
		
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
			//here obtain the IP, port of the namenode, so that we can register to the services 
			//provided by the namenode
			Registry registry=LocateRegistry.getRegistry(Constants.NAME_NODE_IP,Registry.REGISTRY_PORT);
			INameNode nameStub;
			nameStub=(INameNode) registry.lookup(Constants.NAME_NODE);
			responseArray = nameStub.openFile(openFileReqObj.build().toByteArray());
			
			try {
				//Get is the read functionality from the HDFS file system
				OpenFileResponse responseObj = OpenFileResponse.parseFrom(responseArray);
				if(responseObj.getStatus()==Constants.STATUS_NOT_FOUND)
				{
					System.out.println("File not found fatal error");
					System.exit(0);
				}
				
				//receives all the block numbers associated with a given file
				List<String> blockNums = responseObj.getBlockNumsList();
				BlockLocationRequest.Builder blockLocReqObj = BlockLocationRequest.newBuilder();
				
				/**Now, all blocks related to that file is present with us  
				 * in blockNums
				 * perform Read Block Request  from all the blockNums**/
				
				blockLocReqObj.addAllBlockNums(blockNums);
												
				try {
					responseArray = nameStub.getBlockLocations(blockLocReqObj.build().toByteArray());
				} catch (RemoteException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				
				
				
				BlockLocationResponse blockLocResObj = BlockLocationResponse.parseFrom(responseArray);
				
				/**block location response returns all the block locations of each of the 
				 * blocks that were sent by blockNums
				 */
				
				if(blockLocResObj.getStatus()==Constants.STATUS_FAILED)
				{
					System.out.println("Fatal error!");
					System.exit(0);
				}
				
				/** We might have many blocks per file, so we get the datanode locations of each
				 * block as a response to the request nameStub.getBlockLocations
				 */
				List<BlockLocations> blockLocations =  blockLocResObj.getBlockLocationsList();
				
				
				for(int i=0;i<blockLocations.size();i++)
				{
					BlockLocations thisBlock = blockLocations.get(i);
					
					String blockNumber = thisBlock.getBlockNumber();					
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
//							System.out.println("Remote Exception");
							dataNodeCounter++;
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
					//fileWriteObj.writeonly(str);
					fileWriteObj.writeBytes(responseArray);

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

					System.out.println("No of blocks are "+no_of_blocks);
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
						
						String blockNumber = blkLocation.getBlockNumber();
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
					closeFileObj.setDecision(Constants.STATUS_NOT_FOUND);
					
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
	/*public static byte[] read32MBfromFile(int offset)
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
		
	}*/
	
	public static byte[] read32MBfromFile(int offset)
	{
		
		System.out.println("offset is "+offset);

		
		FileInputStream breader = null;
		try {
			breader = new FileInputStream(fileName);
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
		
		
		
		byte[] newCharArray = new byte[bytesToBeRead];
		
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
		return newCharArray;
		
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
