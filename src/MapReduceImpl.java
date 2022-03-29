import java.util.Collection;

public class MapReduceImpl implements MapReduce {
	
	public MapReduceImpl()
	{
	}
	
	public void executeMap(String blockin, String blockout) {
		WordCount wc = new WordCount();
		wc.executeMap(blockin, blockout);	
	}
	
	public void executeReduce(Collection<String> blocks, String finalresults) {
		WordCount wc = new WordCount();
		wc.executeReduce(blocks, finalresults);
	}

}
