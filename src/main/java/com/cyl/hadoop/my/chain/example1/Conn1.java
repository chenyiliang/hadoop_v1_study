package com.cyl.hadoop.my.chain.example1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class Conn1 implements WritableComparable<Conn1> {
	public long orderKey;
	public long customer;
	public String state;
	public double price;
	public Date orderDate;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(orderKey);
		out.writeLong(customer);
		Text.writeString(out, state);
		out.writeDouble(price);
		Text.writeString(out, DateUtil.getDateStr(orderDate, "yyyy-MM-dd"));
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		orderKey = in.readLong();
		customer = in.readLong();
		state = Text.readString(in);
		price = in.readDouble();
		orderDate = DateUtil.getDateFromString(Text.readString(in),
				"yyyy-MM-dd");
	}

	@Override
	public int compareTo(Conn1 conn1) {
		return 0;
	}

}
