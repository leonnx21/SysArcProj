import java.io.*;
import java.net.*;
import java.rmi.*;


public class Launch extends Thread {
	//static int originnodesocket[] = {1000, 1001, 1002};
	static int socketports[] = {2000, 2001, 2002};
	static int rmiports[] = {3000, 3001, 3002};
	static String Nodedata[] = {"Node0.txt", "Node1.txt", "Node2.txt"};
	static String Noderesult[] = {"NodeResult0.txt", "NodeResult1.txt", "NodeResult2.txt"};
	//static String Noderesultatmaster[] = {"MNodeResult0.txt", "MNodeResult1.txt", "MNodeResult2.txt"};
	int node;
	
	public Launch(int node)
	{
		this.node = node;
	}
	
	public void run()
	{
		try {
			Daemon d = (Daemon)Naming.lookup("//localhost:"+rmiports[node]+"/Daemon");	//look up for functions on daemon
			
			MapReduce m = new MapReduceImpl();//initialize MapReduce and callback object on original node
			CallBack cb = new CallBackImpl(3);
			
			d.call(m ,Nodedata[node], Noderesult[node], cb); //invoke call function on Daemon, passed Map Reduced and call back as serializable object
			
			/*
			//start receiving result not ok yet
			ServerSocket ss = new ServerSocket(originnodesocket[node]);
			
			while(true)
				{
					receiveresult(ss.accept());
				}
			*/
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
/*	
	private static void receiveresult(Socket s) //function to receive result
	{
		try {	
			//clean old data
			File oldfile = new File(Noderesultatmaster[node]);
			oldfile.delete();
				
			//get new file
			InputStream is = s.getInputStream();
			BufferedReader bis = new BufferedReader(new InputStreamReader(is));					
						
			while (true) {
				String line = bis.readLine();
				if (line == null){
					s.close();
					System.out.println("File received");
					return;
				}				
				else {writetofile(line+"\n");}
				}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static void writetofile(String line)
	{
		try {
			FileWriter fw = new FileWriter(Noderesultatmaster[node], true); 
	        BufferedWriter bw = new BufferedWriter(fw);
	        bw.write(line);
	        bw.close();
		} catch (Exception e) {e.printStackTrace();}
	}
	
*/	
		
	public static void main(String args[])
	{	

		for (int i=0; i<3; i++) //call Daemon on three threads
		{
			Launch Thread = new Launch(i);
			Thread.start();
		}
		
		/*
		//after wait
		MapReduce m = new MapReduceImpl();
		Collection<String> c =new ArrayList();//mock for now
		c.add("Result1.txt");
		c.add("Result2.txt");
		c.add("Result3.txt");
		
		m.executeReduce(c,"FinalResult.txt"); //execute 
		*/
}
}
