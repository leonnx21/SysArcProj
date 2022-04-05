import java.io.*;
import java.net.*;

public class Split extends Thread {
	static int socketports[] = {2000, 2001, 2002};
	static String originfile;
	static String block[] = {"originblock0.txt","originblock1.txt","originblock2.txt"};
	int targetnode;
	
	public Split(int targetnode) {
		this.targetnode = targetnode;
	}
	
	
	private static void splitfile(String originfile) {//function to split file into block - ok
		try {
			//cleanup old file
			for (int i = 0; i<3; i++)
			{
				File oldfile = new File(originfile+block[i]);
				oldfile.delete();
			}
			
			//loop through to count lines
			BufferedReader buff = new BufferedReader(new FileReader(originfile));
			String line;
			int linecount = 0 ;

			FileWriter fw0 = new FileWriter(originfile+block[0], true); 
	        BufferedWriter bw0 = new BufferedWriter(fw0);
	        FileWriter fw1 = new FileWriter(originfile+block[1], true); 
	        BufferedWriter bw1 = new BufferedWriter(fw1);
			FileWriter fw2 = new FileWriter(originfile+block[2], true); 
	        BufferedWriter bw2 = new BufferedWriter(fw2);
			
			//split to file line by line
			while ((line = buff.readLine()) != null)
			{
				if((linecount % 3)==0)
				{
			        bw0.write(line+"\n");
				}
				else if ((linecount % 2)==0)
				{	
			        bw1.write(line+"\n");
				}
				else
				{
			        bw2.write(line+"\n");
				}
				linecount++;
			}
			buff.close();
			bw0.close();
			bw1.close();
			bw2.close();
	
		} catch (Exception e) {e.printStackTrace();}
	}
	
	
	public void run() {//thread to send to Daemon - ok
		try {
			Socket sc = new Socket ("localhost", socketports[targetnode]);
			OutputStream os = sc.getOutputStream();
			
			FileInputStream fis = new FileInputStream(originfile+block[targetnode]);
			BufferedInputStream is = new BufferedInputStream(fis);
			
			byte[] buff = new byte[1024];
	        int nb;

	        while ((nb = is.read(buff)) > 0) {
	            os.write(buff, 0, nb);
	        }
	        
			sc.close();
			is.close();
			//fis.close();
	
			System.out.println("Data sent to node "+targetnode);
			
		} catch (Exception e) {e.printStackTrace();}
	
	}


	public static void main(String[] args)
	{
		originfile = args[0];
		
		//split file
		splitfile(originfile);

		//send to Daemon on 3 threads		
		for(int i = 0; i<3; i++)
			{
				Split thread = new Split(i);
				thread.start();
			}


	}
	
	
	
}

