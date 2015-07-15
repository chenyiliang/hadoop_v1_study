package com.cyl.hadoop.fiftyone.join3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class UserKey implements WritableComparable<UserKey> {
	private int keyid;
	private boolean isPrimary;

	public int getKeyid() {
		return keyid;
	}

	public void setKeyid(int keyid) {
		this.keyid = keyid;
	}

	public boolean isPrimary() {
		return isPrimary;
	}

	public void setPrimary(boolean isPrimary) {
		this.isPrimary = isPrimary;
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeInt(this.keyid);
		output.writeBoolean(this.isPrimary);
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.keyid = input.readInt();
		this.isPrimary = input.readBoolean();
	}

	@Override
	public int compareTo(UserKey o) {
		if (this.keyid == o.keyid) {
			if (this.isPrimary == o.isPrimary) {
				return 0;
			} else {
				return this.isPrimary ? 1 : -1;
			}
		} else {
			return this.keyid > o.keyid ? 1 : -1;
		}
	}

	@Override
	public String toString() {
		return "UserKey [keyid=" + keyid + ", isPrimary=" + isPrimary + "]";
	}

}
