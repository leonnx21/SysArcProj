import java.io.*;
import java.net.*;
import java.rmi.*;
import java.util.ArrayList;
import java.util.Collection;

public class Launch extends Thread {
	static int launchsocket = 1000;
	static int socketports[] = {2000, 2001, 2002};
	static int rmiports[] = {3000, 3001, 3002};
	static String Nodedata[] = {"Node0.txt", "Node1.txt", "Node2.txt"};
	static String Noderesult[] = {"NodeResult0.txt", "NodeResult1.txt", "NodeResult2.txt"};
	static String Noderesultatmaster[] = {"MNodeResult0.txt", "MNodeResult1.txt", "MNodeResult2.txt"};
	int node;
	static ServerSocket ss;
	static CallBack cb;
	static MapReduce m;
	
	public Launch(int node)
	{		
		try {
			this.node = node;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void run() // ok
	{
		try {
			Daemon d = (Daemon)Naming.lookup("//localhost:"+rmiports[node]+"/Daemon");	//look up for functions on daemon
			
			d.call(m ,Nodedata[node], Noderesult[node], cb); //invoke call function on Daemon, passed Map Reduced  as Serializable object and call back as remote  object

			receiveresult(node);//get back result file
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	private void receiveresult(int node)
	{
		try {	
			//clean old data
			File oldfile = new File(Noderesultatmaster[node]);
			oldfile.delete();
				
			//get new file
			Socket s = ss.accept();
			InputStream is = s.getInputStream();
			BufferedInputStream bis = new BufferedInputStream(is);
			
			FileOutputStream fos = new FileOutputStream(Noderesultatmaster[node]);
			OutputStream os = new BufferedOutputStream(fos);
		
		    byte[] buffer = new byte[1024];
		    int lengthRead;
		        
		    while ((lengthRead = bis.read(buffer)) > 0) {
		            os.write(buffer, 0, lengthRead);
		            os.flush();
		        }
		    
			os.close();	
			s.close();
			System.out.println("result received from node "+node);
							
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

		
	public static void main(String[] args)
	{			

		final long startTime = System.currentTimeMillis();
		
		try {
			ss = new ServerSocket(launchsocket);
			m = new MapReduceImpl();//initialize Map Reduce and Callback on Launch
			cb = new CallBackImpl(3);

		
		for (int i=0; i<3; i++) //call Daemon on three threads
		{
			Launch Thread= new Launch(i);
			Thread.start();
		}
		
		((CallBackImpl) cb).waitforall();
		
		//after wait
		Collection<String> c = new ArrayList<String>();
		
		for (int i=0; i<3; i++)
		{
			c.add(Noderesultatmaster[i]);
		}
		
		m.executeReduce(c,"FinalResult.txt"); //execute 
		System.out.println("Map Reduce completed: FinalResult.txt");
		ss.close();
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	
		final long endTime = System.currentTimeMillis();

		System.out.println("Total execution time: " + (endTime - startTime));
		
		System.exit(0);
	}
}
