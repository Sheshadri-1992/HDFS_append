package com.hdfs.namenode;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.protobuf.InvalidProtocolBufferException;
import com.hdfs.miscl.Constants;
import com.hdfs.miscl.Hdfs.AssignBlockRequest;
import com.hdfs.miscl.Hdfs.AssignBlockResponse;
import com.hdfs.miscl.Hdfs.BlockLocationRequest;
import com.hdfs.miscl.Hdfs.BlockLocationResponse;
import com.hdfs.miscl.Hdfs.BlockLocations;
import com.hdfs.miscl.Hdfs.BlockReportRequest;
import com.hdfs.miscl.Hdfs.BlockReportResponse;
import com.hdfs.miscl.Hdfs.CloseFileRequest;
import com.hdfs.miscl.Hdfs.CloseFileResponse;
import com.hdfs.miscl.Hdfs.DataNodeLocation;
import com.hdfs.miscl.Hdfs.HeartBeatRequest;
import com.hdfs.miscl.Hdfs.HeartBeatResponse;
import com.hdfs.miscl.Hdfs.ListFilesRequest;
import com.hdfs.miscl.Hdfs.ListFilesResponse;
import com.hdfs.miscl.Hdfs.OpenFileRequest;
import com.hdfs.miscl.Hdfs.OpenFileResponse;

public class NameNodeDriver implements INameNode
{

	public static HashMap<Integer,DataNodeLocation>  dataNodes;   //data node id, location
	public static HashMap<Integer,List<DataNodeLocation>> blockLocations;
	public static HashMap<Integer,Long>  heartBeatDataNodes; 
	public static PutFile putFile ;
	public static GetFile getFile;
	public static int numBlock=0;
	
	public static void main(String[] args) {

		System.setProperty("java.security.policy","./security.policy");
		//set the security manager
        if (System.getSecurityManager() == null) {
            System.setSecurityManager(new SecurityManager());
        }
		
		dataNodes = new HashMap<>();
		blockLocations = new HashMap<>();
		heartBeatDataNodes = new HashMap<>();
		
		putFile = new PutFile();
		getFile = new GetFile();
		
		bindToRegistry();
		checkIfDataNodeIsAlive();
		
	}


	
	static void bindToRegistry()
	{
		System.setProperty("java.rmi.server.hostname",Constants.NAME_NODE_IP);
		NameNodeDriver obj = new NameNodeDriver();
		try {
			
			Registry register=LocateRegistry.createRegistry(Registry.REGISTRY_PORT);
			INameNode stub = (INameNode) UnicastRemoteObject.exportObject(obj,Registry.REGISTRY_PORT);
			try {
				register.bind(Constants.NAME_NODE, stub);
				
				System.out.println("Name Node started succesfully");
			} catch (AlreadyBoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Name Node failed to bind");
				e.printStackTrace();
			}
			
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}



	@Override
	public byte[] openFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
		
		System.out.println("Open file called");
		OpenFileResponse.Builder res = OpenFileResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		
		OpenFileRequest req;
		try {
			req = OpenFileRequest.parseFrom(inp);
			
			String fileName = req.getFileName();
			boolean type = req.getForRead();
			
			if(!type)   // type = false then write i.e. put
			{
				if(getFile.getFileDetails(fileName)==null)
				{
					int fileHandle = new Random().nextInt()%100000;
					fileHandle = Math.abs(fileHandle);
					System.out.println(fileHandle);
				    putFile.insertFileHandle(fileName, fileHandle);
				    
				    res.setHandle(fileHandle);
				    res.setStatus(Constants.STATUS_SUCCESS);
				}
				else
				{
					res.setStatus(Constants.STATUS_NOT_FOUND);
				}
				
			   
				
				
			}else       // type = true then read i.e get
			{
				Integer[] blocks = getFile.getFileDetails(fileName);
				if(blocks==null)
				{
					res.setStatus(Constants.STATUS_NOT_FOUND);
				}else
				{
					res.setStatus(Constants.STATUS_SUCCESS);
					Iterable<Integer> iterable = Arrays.asList(blocks);
					res.addAllBlockNums(iterable);
				}
				
				
			}
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] closeFile(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		CloseFileRequest req = null;
		CloseFileResponse.Builder res = CloseFileResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		
		try {
			req = CloseFileRequest.parseFrom(inp);
			res.setStatus(Constants.STATUS_SUCCESS);
			
			Integer handle = req.getHandle();
			
			if(handle != null)
			{
				putFile.removeFileHandle(handle);
				
			}
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] getBlockLocations(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("Block location called");
		
		BlockLocationResponse.Builder res = BlockLocationResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		
		try {
			BlockLocationRequest req = BlockLocationRequest.parseFrom(inp);
			
			List<Integer> blocks = req.getBlockNumsList();
			
			List<BlockLocations> locs  = getFile.getBlockLocations(blocks,blockLocations);
			
			res.setStatus(Constants.STATUS_SUCCESS);
			res.addAllBlockLocations(locs);
			
		
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();
	}

	@Override
	public byte[] assignBlock(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
		System.out.println("Assign block called");
		AssignBlockResponse.Builder res = AssignBlockResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		
		try {
			AssignBlockRequest req = AssignBlockRequest.parseFrom(inp);
			
			int handle = req.getHandle();
			int numBlock = getBlockNum();
			
			putFile.insertFileBlock(handle, numBlock);
			
			
			res.setStatus(Constants.STATUS_SUCCESS);
			
			BlockLocations.Builder blocks =  BlockLocations.newBuilder();
			
			
			
			int max = dataNodes.values().size();
			
			System.out.println("Data nodes size "+max);
			
			if(max==1)
			{
				DataNodeLocation value = (DataNodeLocation) dataNodes.values().toArray()[0];
				blocks.addLocations(value);
//				System.out.println("Value is "+value);
			}else
			{
				int [] randoms = getTwoRandoms(max); 
				
				List<Integer> keys      = new ArrayList<Integer>(dataNodes.keySet());
				Integer randomKey = keys.get( randoms[0]);
				DataNodeLocation value     = dataNodes.get(randomKey);
				blocks.addLocations(value);
				
				randomKey = keys.get( randoms[1]);
				value = dataNodes.get(randomKey);
				blocks.addLocations(value);
			}
			
	
//			System.out.println("Num block "+numBlock);
			blocks.setBlockNumber(numBlock);
			res.setNewBlock(blocks);
			
			System.out.println("Response" + res);
			
			return res.build().toByteArray();
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] list(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub

		ListFilesResponse.Builder res = ListFilesResponse.newBuilder();
		res.setStatus(Constants.STATUS_FAILED);
		try {
			ListFilesRequest req = ListFilesRequest.parseFrom(inp);
			
			res.addAllFileNames(getFile.getAllFileNames());
			res.setStatus(Constants.STATUS_SUCCESS);
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();
	}



	@Override
	public byte[] blockReport(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
//		System.out.println("Block report called");
		BlockReportResponse.Builder res = BlockReportResponse.newBuilder();
		
		try {
			BlockReportRequest req = BlockReportRequest.parseFrom(inp);
			
			int id = req.getId();
			DataNodeLocation loc = req.getLocation();
			
//			System.out.println("Data node " + req.getBlockNumbersCount());
			
			dataNodes.put(id,loc);
			
			for(int i=0;i<req.getBlockNumbersCount();i++)
			{
				int numBlock = req.getBlockNumbers(i);
				if(!blockLocations.containsKey(numBlock))
				{
					
					List<DataNodeLocation> arrLoc = new ArrayList<DataNodeLocation>();
					arrLoc.add(loc);
//					System.out.println("Added block "+numBlock);
					
					blockLocations.put(numBlock, arrLoc);
					
				}else
				{
					List<DataNodeLocation> tmpLoc = blockLocations.get(numBlock);
					
					if(tmpLoc.size()!=2)
					{
						if(!tmpLoc.get(0).equals(loc))
						{
							tmpLoc.add(loc);
						}
					}
				}
				
			}
			
//			System.out.print(dataNodes.get(id));
			
			res.addStatus(Constants.STATUS_SUCCESS);
			
			
			
			
			
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();

	}



	@Override
	public byte[] heartBeat(byte[] inp) throws RemoteException {
		// TODO Auto-generated method stub
		
//	System.out.println("Got heart beat " + new Date().getTime());
		HeartBeatResponse.Builder res  = HeartBeatResponse.newBuilder();
		res.setStatus(Constants.STATUS_SUCCESS);
		try {
			HeartBeatRequest req = HeartBeatRequest.parseFrom(inp);
			heartBeatDataNodes.put(req.getId(), new Date().getTime());
			
		} catch (InvalidProtocolBufferException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return res.build().toByteArray();
	}
	
	
	
	public static int getRandom(int max)
	{
		int random;
		Random rand =new Random();
		int min=0;
		random = rand.nextInt((max - min) + 1) + min;
		
		return random;
	}
	
	
	public static synchronized int getBlockNum()
	{
		
		
		try {
			
			BufferedReader buff = new BufferedReader(new FileReader(Constants.BLOCK_NUM_FILE));
			String line=buff.readLine();
			buff.close();
			
			Integer num = Integer.parseInt(line);
			num++;
			PrintWriter pw;
			try {
				pw = new PrintWriter(new FileWriter(Constants.BLOCK_NUM_FILE));
			    pw.write(num.toString());
		        pw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return num-1;
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return ++numBlock;
	}
	


	private int[] getTwoRandoms(int max) {
		// TODO Auto-generated method stub
		int [] randoms = new int[2];
		Random rand =new Random();
		int min=0;
		
		int random = rand.nextInt((max - min)) + min;
		int random2;
		
		randoms[0]=random;
		
		do
		{
			random2 = rand.nextInt((max - min)) + min;
		}
	    while(random==random2) ;
		
		randoms[1] = random2;
		
		return randoms;
	}
	
	
	private static void checkIfDataNodeIsAlive()
	{
		
		new Thread(new Runnable() {
			
			@Override
			public void run() {
				// TODO Auto-generated method stub
				
			
				
				while(true)
				{
//					System.out.print("hello");
					List<Integer> remove = new ArrayList<>();
					for(Map.Entry<Integer,Long > entry : heartBeatDataNodes.entrySet())
					{
//						System.out.println("current heartbeat datanode "+new Date().getTime() + entry.getValue() + entry.getKey());
						if(new Date().getTime() - entry.getValue() > Constants.HEART_BEAT_FREQ)
						{
							dataNodes.remove(entry.getKey());
							System.out.println("Data node  " + entry.getKey() + " is dead");
							remove.add(entry.getKey());
							
						}
					}
					
					for(Integer key : remove)
					{
						heartBeatDataNodes.remove(key);
					}
					
					
					try {
						Thread.sleep(Constants.HEART_BEAT_FREQ+1000);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				
				
			}
		}).start();
		
		
		
	}
	
	
}
