import java.io.*;
import java.net.*;
import java.rmi.*;
import java.util.ArrayList;
import java.util.Collection;

public class Launch extends Thread {
	static int originnodesocket[] = {1000, 1001, 1002};
	static int socketports[] = {2000, 2001, 2002};
	static int rmiports[] = {3000, 3001, 3002};
	static String Nodedata[] = {"Node0.txt", "Node1.txt", "Node2.txt"};
	static String Noderesult[] = {"NodeResult0.txt", "NodeResult1.txt", "NodeResult2.txt"};
	static String Noderesultatmaster[] = {"MNodeResult0.txt", "MNodeResult1.txt", "MNodeResult2.txt"};
	int node;
	ServerSocket ss;
	static CallBack cb;
	
	public Launch(int node)
	{		
		try {
			this.node = node;
			this.ss = new ServerSocket(originnodesocket[node]);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void run() //seem to be ok
	{
		try {
			Daemon d = (Daemon)Naming.lookup("//localhost:"+rmiports[node]+"/Daemon");	//look up for functions on daemon
			
			MapReduce m = new MapReduceImpl();//initialize MapReduce to transfer to Daemon
			
			d.call(m ,Nodedata[node], Noderesult[node], cb); //invoke call function on Daemon, passed Map Reduced  as Serializable object and call back as remote  object

			//get back result file
			try {	
				//clean old data
				File oldfile = new File(Noderesultatmaster[node]);
				oldfile.delete();
					
				//get new file
				Socket s = ss.accept();
				InputStream is = s.getInputStream();
				BufferedReader bis = new BufferedReader(new InputStreamReader(is));					
				
				boolean accept = true;
				
				while (accept == true) {
					String line = bis.readLine();
					if (line == null){
						ss.close();
						System.out.println("result received from node "+node);
						accept = false;
					}				
					else {writetofile(line+"\n", node);}
					}
				
								
			} catch (Exception e) {
				e.printStackTrace();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private static void writetofile(String line, int node)
	{
		try {
			FileWriter fw = new FileWriter(Noderesultatmaster[node], true); 
	        BufferedWriter bw = new BufferedWriter(fw);
	        bw.write(line);
	        bw.close();
		} catch (Exception e) {e.printStackTrace();}
	}

		
	public static void main(String args[])
	{			

		try {
			cb = new CallBackImpl(3);
		} catch (RemoteException e) {
			e.printStackTrace();
		}

		for (int i=0; i<3; i++) //call Daemon on three threads
		{
			Launch Thread= new Launch(i);
			Thread.start();
			try {
				Thread.join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		
		//after wait
		MapReduce m = new MapReduceImpl();
		Collection<String> c = new ArrayList();
		
		for (int i=0; i<3; i++)
		{
			c.add(Noderesult[i]);
		}
	
		m.executeReduce(c,"FinalResult.txt"); //execute 
		System.out.println("Map Reduce completed");
		
		return;
}
}
