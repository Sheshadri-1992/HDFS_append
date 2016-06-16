package com.hdfs.miscl;

import java.io.*;

public class TestChunks {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		

	}
	
	public static byte[] toByteArray(File file, long start, long count) {
	      long length = file.length();
	      if (start >= length) return new byte[0];
	      count = Math.min(count, length - start);
	      byte[] array = new byte[(int)count];
	      InputStream in = null;
		try {
			in = new FileInputStream(file);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      try {
			in.skip(start);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      long offset = 0;
	      while (offset < count) {
	          int tmp = 0;
			try {
				tmp = in.read(array, (int)offset, (int)(length - offset));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	          offset += tmp;
	      }
	      try {
			in.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	      return array;
	}

}
