package dj.storm.trident.partition.aggregate;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.storm.trident.state.State;

import dj.storm.utils.FileStream;

/**
 * 
 * @author Adolf.Felix <b>jonguo@yeah.net</b>
 */
public class SumState implements State {
	private Long curTxid;
	private Boolean isUpdate;
	
	private FileStream fileStream = new FileStream();
	
	private AtomicBoolean flag = new AtomicBoolean(false);
	private static AtomicInteger Index = new AtomicInteger(0);
	
	@Override
	public void beginCommit(Long txid) {
		if (flag.compareAndSet(false, true)) {
			fileStream.initalOutput("sum-state-" + Index.getAndAdd(1) + ".out");
		}
		fileStream.write("++++ beginCommit(time=" + System.currentTimeMillis() + ") ++++");
		fileStream.write("++++ beginCommit(txid="+txid+") ++++");
		
		if (this.curTxid != txid) {
			this.isUpdate = false;
		}
		this.curTxid = txid;
	}

	@Override
	public void commit(Long txid) {
		fileStream.write("++++ commit(time=" + System.currentTimeMillis() + ") ++++");
		fileStream.write("++++ commit(curTxid="+this.curTxid+ ", txid=" + txid + ", isUpdate="+ this.isUpdate +") ++++\n");
	}
	
	public Long getCurTxid() {
		return curTxid;
	}
	public void setIsUpdate(Boolean isUpdate) {
		this.isUpdate = isUpdate;
	}
}
