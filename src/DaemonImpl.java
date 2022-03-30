import java.io.*;
import java.net.*;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

public class DaemonImpl extends UnicastRemoteObject implements Daemon{
	static int originnodesocket[] = {1000, 1001, 1002};
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
		senddata();
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
			BufferedReader bis = new BufferedReader(new InputStreamReader(is));					
			String line;
			
			while ((line = bis.readLine()) != null) {		
				writetofile(line);
				}
	
			s.close();
			System.out.println("File received");


		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	private static void writetofile(String line)
	{
		try {
			FileWriter fw = new FileWriter(Nodedata[node], true); 
	        BufferedWriter bw = new BufferedWriter(fw);
	        bw.write(line+"\n");
	        bw.close();
		} catch (Exception e) {e.printStackTrace();}
	}
	
	
	public static void senddata() {//send to Launch 
		try {
			Socket sc = new Socket ("localhost", originnodesocket[node]);
			OutputStream os = sc.getOutputStream();
		
			byte buff[] = new byte[1024];
			FileInputStream fis = new FileInputStream(Noderesult[node]);
			
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
