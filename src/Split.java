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

			//split to file line by line
			while ((line = buff.readLine()) != null)
			{
				if((linecount % 3)==0)
				{
					writetofile(originfile+block[2], line);
				}
				else if ((linecount % 2)==0)
				{
					writetofile(originfile+block[1], line);
				}
				else
				{
					writetofile(originfile+block[0], line);
				}
				linecount++;
			}
			
			buff.close();
	
		} catch (Exception e) {e.printStackTrace();}
	}
	
	
	private static void writetofile(String filename, String line) //function to write to file - ok
	{
		try {
			FileWriter fw = new FileWriter(filename, true); 
	        BufferedWriter bw = new BufferedWriter(fw);
	        bw.write(line+"\n");
	        bw.close();
		} catch (Exception e) {e.printStackTrace();}
	}
	
	
	public void run() {//thread to send to Daemon - ok
		try {
			Socket sc = new Socket ("localhost", socketports[targetnode]);
			OutputStream os = sc.getOutputStream();
			
			byte buff[] = new byte[1024];
			FileInputStream fis = new FileInputStream(originfile+block[targetnode]);
			
			while(true)
			{
				int nb = fis.read(buff);
				if (nb == -1) {
					break;
				}
				os.write(buff);
			}
			
			sc.close();
			fis.close();
	
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

