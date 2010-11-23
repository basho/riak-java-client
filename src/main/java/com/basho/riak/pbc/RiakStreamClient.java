/**
 * This file is part of riak-java-pb-client 
 *
 * Copyright (c) 2010 by Trifork
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.basho.riak.pbc;

import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.TimerTask;

abstract class RiakStreamClient<T> implements Iterable<T> {

	private RiakClient client;
	protected RiakConnection conn;
	private ReaperTask reaper;

	protected RiakStreamClient(RiakClient client, RiakConnection conn) {
		this.client = client;
		this.conn = conn;
		this.reaper = new ReaperTask(this, conn);
	}

	static class ReaperTask extends TimerTask {

		private final RiakConnection conn;
		private WeakReference<?> ref;

		ReaperTask (Object holder, RiakConnection conn) {
			this.conn = conn;
			this.ref = new WeakReference<Object>(holder);
			RiakConnection.timer.scheduleAtFixedRate(this, 1000, 1000);
		}
		
		@Override
		public synchronized void run() {
			if (ref == null) { 
				// do nothing; we were explicitly cancelled //
			} else if (ref.get() == null) {
				
				// the reference was lost; cancel this timer and
				// close the connection
				cancel();
				conn.close();
			} else if (conn.isClosed()) {
				cancel();
			}
		}

		@Override
		public synchronized boolean cancel() {
			ref = null;
			return super.cancel();
		}
	}
	
	public synchronized void close() {
		if (!isClosed()) {
			reaper.cancel();
			client.release(conn);
			conn = null;
		}
	}
	
	public boolean isClosed() {
		return conn == null;
	}

	public abstract boolean hasNext() throws IOException;
	public abstract T next() throws IOException; 

	@Override
	public Iterator<T> iterator() {
		return new Iterator<T>() {

			@Override
			public boolean hasNext() {
				try {
					return RiakStreamClient.this.hasNext();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public T next() {
				try {
					return RiakStreamClient.this.next();
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}
}
