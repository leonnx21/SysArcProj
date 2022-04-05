import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
	int nbnode;
	
	public CallBackImpl(int n) throws RemoteException { 
		nbnode = n; 
	}

	public synchronized void completed() throws RemoteException {
		System.out.println("Notify completion");
		notify();		
	}
	
	public synchronized void waitforall() throws InterruptedException {
		for (int i=0 ; i<nbnode; i++)
		{
			wait();
		}
			
			}
}
