package zx.soft.naive.bayes.mapred.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

public class DbOutputWritable implements Writable, DBWritable {

	private long wid; // 微博内容
	private String cate; // 类别

	public DbOutputWritable(long wid, String cate) {
		this.wid = wid;
		this.cate = cate;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		wid = in.readLong();
		cate = Text.readString(in);
	}

	@Override
	public void readFields(ResultSet rs) throws SQLException {
		wid = rs.getLong(1);
		cate = rs.getString(2);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(wid);
		Text.writeString(out, cate);
	}

	@Override
	public void write(PreparedStatement ps) throws SQLException {
		ps.setLong(1, wid);
		ps.setString(2, cate);
	}

}
