import java.io.*;
import java.net.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class DaemonImpl extends UnicastRemoteObject implements Daemon{
	static int launchsocket = 1000;
	static int socketports[] = {2000, 2001, 2002};
	static int rmiports[] = {3000, 3001, 3002};
	static String Nodedata[] = {"Node0.txt", "Node1.txt", "Node2.txt"};
	static String Noderesult[] = {"NodeResult0.txt", "NodeResult1.txt", "NodeResult2.txt"};
	static int node;

	
	public DaemonImpl() throws RemoteException
	{
	}
	
	public void call(MapReduce m, String blockin, String blockout, CallBack cb) 
			throws RemoteException{
		m.executeMap(blockin, blockout);
		System.out.println("Map Executed");
		sendresult();
		cb.completed();
	}
	
	private static void receivedata(Socket s) //function to receive data block - ok
	{
		try {	
			//clean old data
			File oldfile = new File(Nodedata[node]);
			oldfile.delete();
				
			//get new file
			InputStream is = s.getInputStream();	
			BufferedInputStream bis = new BufferedInputStream(is);
			
			FileOutputStream fos = new FileOutputStream(Nodedata[node]);
			OutputStream os = new BufferedOutputStream(fos);
		
		    byte[] buffer = new byte[1024];
		    int lengthRead;
		        
		    while ((lengthRead = bis.read(buffer)) > 0) {
		            os.write(buffer, 0, lengthRead);
		            os.flush();
		        }
			os.close();
			
			System.out.println("File received");


		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	
	public static void sendresult() {//send to Launch 
		try {
			Socket sc = new Socket ("localhost", launchsocket);
			
			OutputStream os = sc.getOutputStream();
	
			FileInputStream fis = new FileInputStream(Noderesult[node]);
			InputStream is = new BufferedInputStream(fis);
			
			byte[] buff = new byte[1024];
	        int nb;
	        
	        while ((nb = is.read(buff)) > 0) {
	            os.write(buff, 0, nb);
	        }
			
			sc.close();
			is.close();
			
			System.out.println("result sent from node "+node);
			
		} catch (Exception e) {e.printStackTrace();}
	
	}

	
	public static void main(String args[]) {
		node = Integer.parseInt(args[0]);

		try {
			LocateRegistry.createRegistry(rmiports[node]);	
			Naming.bind("//localhost:"+rmiports[node]+"/Daemon", new DaemonImpl());

			ServerSocket ss = new ServerSocket(socketports[node]); //ok
			System.out.println("Node "+node+" ready");
			
			while(true)
			{
				receivedata(ss.accept());
			}
			
		} catch (Exception e) {e.printStackTrace();}

	}


}
