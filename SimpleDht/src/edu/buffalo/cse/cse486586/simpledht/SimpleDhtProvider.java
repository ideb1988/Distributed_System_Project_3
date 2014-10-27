package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;


public class SimpleDhtProvider extends ContentProvider 
{

	// CLASS VARIABLES

	public static TreeMap <String, String> nodes = new TreeMap <String, String>();
	public static TreeMap <String, String> sNodes = new TreeMap <String, String>();
	public static TreeMap <String, String> pNodes = new TreeMap <String, String>();
	public static int noOfNodes = 0;
	public static String sNode;
	public static String pNode;
	public static String node;
	public static Cursor finalCursor = null;
	public static Boolean deleteLock;
	public static Boolean queryLock;

	// METHODS

	// ON CREATE FUNCTION

	public boolean onCreate() 
	{
		// FILD MY PORT

		TelephonyManager tel = (TelephonyManager) getContext().getSystemService(Context.TELEPHONY_SERVICE);
		String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		final String myPort = String.valueOf((Integer.parseInt(portStr) * 2));

		// OPEN A NEW SERVERSOCKET ON AN ASYNC THREAD

		ServerTask serverTask;
		try 
		{
			ServerSocket serverSocket = new ServerSocket(10000);
			serverTask = new ServerTask(myPort, 
					buildUri("content", "edu.buffalo.cse.cse486586.simpledht.provider"));

			serverTask.executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			// ASK THE LEADER(5554) TO JOIN THIS NODE TO THE CHORD

			ClientTask clientTask = new ClientTask();		
			clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "JOIN_REQ", myPort, "_VOID_");
		} 
		catch (IOException e) 
		{
			Log.e("ServerSocket", "Can't create a ServerSocket");
		}
		return true;
	}

	// DELETE FROM DATABASE

	public int delete(Uri uri, String selection, String[] selectionArgs) 
	{
		// CHECK THE DELETELOCK AND THE NODEID

		String askingNode;
		if (selectionArgs == null)
		{
			askingNode = node;
			deleteLock = true;
		}
		else
		{
			askingNode = selectionArgs[0];
			deleteLock = false;
		}

		// DELETE FUNC BASED ON SELECTION PREDICATE

		DataBase mydb = new DataBase(getContext() , uri.toString());
		SQLiteDatabase database = mydb.getWritableDatabase();
		if (selection.equals("*"))
		{
			database.delete("[" +uri.toString()+"]", null, null);
			if (noOfNodes > 1)
			{
				ClientTask clientTask = new ClientTask();		
				clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
						"DELETE_ALL", sNode, selection, askingNode);

				while(deleteLock)
				{

				}
			}
		}
		else if (selection.equals("@"))
		{
			database.delete("[" +uri.toString()+"]", null, null);
		}
		else 
		{
			database.delete("[" +uri.toString()+"]", "key = '" + selection + "'", null);
			if (noOfNodes > 1)
			{
				ClientTask clientTask = new ClientTask();		
				clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
						"DELETE_ALL", sNode, selection, askingNode);

				// LOCK ON THE NODE TILL IT IS RELEASED

				while(deleteLock){}
			}
		}
		return 0;
	}

	public String getType(Uri uri) 
	{
		return null;
	}

	// INSERT TO THE CONTENT PROVIDER DATABASE

	public Uri insert(Uri uri, ContentValues values) 
	{
		try
		{
			DataBase mydb = new DataBase(getContext() , uri.toString());
			SQLiteDatabase database = mydb.getWritableDatabase();
			String key = (String) values.get("key");
			String value = (String) values.get("value");
			String hashedKey = genHash(key);
			String nodeHash = "";
			String pNodeHash = "";
			if (noOfNodes > 1)
			{
				nodeHash = genHash(node);
				pNodeHash = genHash(pNode);
			}
			if (noOfNodes == 1 || noOfNodes == 0)
			{
				int check = database.update("["+uri.toString()+"]",
						values, 
						"key = '"+ key.toString() +"'", 
						null);
				if (check == 0)
				{
					database.insert("[" +uri.toString()+"]" , null ,  values);
				}
				database.close();
			}
			else if (noOfNodes >= 2)
			{
				if ((hashedKey.compareTo(pNodeHash) > 0) &&
						(hashedKey.compareTo(nodeHash) <= 0) &&
						(nodeHash.compareTo(pNodeHash) > 0))
				{
					int check = database.update("["+uri.toString()+"]",
							values, 
							"key = '"+ key.toString() +"'", 
							null);
					if (check == 0)
					{
						database.insert("[" +uri.toString()+"]" , null ,  values);
					}
					database.close();
				}
				else if (((hashedKey.compareTo(pNodeHash) > 0) &&
						(hashedKey.compareTo(nodeHash) >= 0) &&
						(pNodeHash.compareTo(nodeHash) > 0)) ||
						((hashedKey.compareTo(pNodeHash) < 0) &&
								(hashedKey.compareTo(nodeHash) <= 0) &&
								(pNodeHash.compareTo(nodeHash) > 0)))
				{
					int check = database.update("["+uri.toString()+"]",
							values, 
							"key = '"+ key.toString() +"'", 
							null);
					if (check == 0)
					{
						database.insert("[" +uri.toString()+"]" , null ,  values);
					}
					database.close();
				}
				else
				{
					ClientTask clientTask = new ClientTask();		
					clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "INS_REQ", 
							sNode, key + ":" + value, nodeHash);
				}
			}
		}
		catch (Exception e)
		{
			Log.e("insert()", "Insert Failed");
		}
		return uri;
	}

	// QUERY

	public Cursor query(Uri uri, String[] projection, String selection, String[] selectionArgs,
			String sortOrder) 
	{
		try
		{
			DataBase mydb = new DataBase(getContext() , uri.toString());
			SQLiteDatabase database = mydb.getWritableDatabase();
			Cursor query = null;
			if (selection.equals("*"))
			{
				query = database.rawQuery("SELECT * " +	"FROM [" +uri.toString()+"] ", null);
				if (noOfNodes <= 1)
				{
					return query;
				}
				String cursorToString = makeString(query);
				finalCursor = null;
				queryLock =  true;
				ClientTask clientTask = new ClientTask();		
				clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
						"QUERY_SEL_ALL", sNode, selection, cursorToString, node);
				while (finalCursor == null || finalCursor.getCount() == 0)
				{
					if (!queryLock)
					{
						break;
					}
				}
				query = finalCursor;
			}
			else if (selection.equals("@"))
			{
				query = database.rawQuery("SELECT * " +	"FROM [" +uri.toString()+"] ", null);
			}
			else
			{
				query = database.rawQuery("SELECT * " +	"FROM [" +uri.toString()+"] " +	"WHERE key = '" + selection +"'", null);
				if (query.getCount() == 0)
				{
					finalCursor = null;
					queryLock = true;
					ClientTask clientTask = new ClientTask();		
					clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
							"QUERY_SEL", sNode, selection, node);

					while (finalCursor == null || finalCursor.getCount() == 0)
					{
						if (!queryLock)
						{
							break;
						}
					}
					query = finalCursor;
				}
			}
			return query;
		}
		catch (Exception e)
		{
			Log.e("query()", "Query Failed");
			return null;
		}
	}

	// UPDATE DATABASE

	public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) 
	{
		return 0;
	}

	// HASH GENERATION

	private String genHash(String input) throws NoSuchAlgorithmException 
	{
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) 
		{
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}

	// URI BUILDER

	private Uri buildUri(String scheme, String authority) 
	{
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	// QUERYHELPER

	public void queryHelper(Uri uri, String selection, String port, String prevCurToString) 
	{
		try
		{
			DataBase mydb = new DataBase(getContext() , uri.toString());
			SQLiteDatabase database = mydb.getWritableDatabase();
			Cursor query = null;
			if (selection.equals("*"))
			{
				query = database.rawQuery("SELECT * " +	"FROM [" +uri.toString()+"] ", null);
				String cursorToString = makeString(query);
				String newCurToString = prevCurToString + cursorToString;
				ClientTask clientTask = new ClientTask();		
				clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
						"QUERY_SEL_ALL", sNode, selection, newCurToString, port);
			}
			else if (selection.equals("@"))
			{
				query = database.rawQuery("SELECT * " +	"FROM [" +uri.toString()+"] ", null);
			}
			else
			{
				query = database.rawQuery("SELECT * " +	"FROM [" +uri.toString()+"] " +	"WHERE key = '" + selection +"'", null);
				if (query.getCount() == 0)
				{
					ClientTask clientTask = new ClientTask();		
					clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
							"QUERY_SEL", sNode, selection, port);
				}
				else
				{
					String cursorToString = makeString(query);
					ClientTask clientTask = new ClientTask();		
					clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
							"QUERY_RES", sNode, cursorToString, port);
				}
			}
		}
		catch (Exception e)
		{
			Log.e("queryHelper()", "Error in Query");
		}
	}

	// CONVERT CURSOR TO STRING

	public String makeString(Cursor query)
	{
		String cursorToString = "";
		if (query.moveToFirst()) 
		{
			while (!query.isAfterLast()) 
			{
				int keyIndex = query.getColumnIndex("key");
				int valueIndex = query.getColumnIndex("value");
				String returnKey = query.getString(keyIndex);
				String returnValue = query.getString(valueIndex);
				cursorToString = cursorToString + returnKey + ":" + returnValue + ":";
				query.moveToNext();
			}
		}
		return cursorToString;
	}

	// INNER SERVERTASK CLASS

	public class ServerTask extends AsyncTask<ServerSocket, String, Void> implements Serializable
	{
		private static final long serialVersionUID = 1L;
		HashMap<String, String> portNAvd= new HashMap<String, String>();
		String myPort;
		Uri uri;

		public ServerTask(String myPort, Uri uri)
		{
			this.portNAvd.put("11108", "5554");
			this.portNAvd.put("11112", "5556");
			this.portNAvd.put("11116", "5558");
			this.portNAvd.put("11120", "5560");
			this.portNAvd.put("11124", "5562");
			this.uri = uri;
			this.myPort = myPort;
		}

		protected Void doInBackground(ServerSocket... sockets) 
		{
			try
			{
				while (true)
				{        	
					ServerSocket serverSocket = sockets[0];
					Socket accept = serverSocket.accept();
					ObjectInputStream is = new ObjectInputStream(accept.getInputStream());
					Object obj;
					try 
					{
						obj = is.readObject();
					}
					catch (ClassNotFoundException e)
					{
						obj = null;
						Log.e("ServerTask/Sockets", "ClassNotFound");
					}
					String recvdMsg = (String) obj;
					publishProgress(recvdMsg);
				}
			}
			catch (IOException e)
			{
				Log.e("ServerSocket", "Accept Failed");
			}
			return null;
		}

		protected void onProgressUpdate(String...recvdMsg) 
		{
			String[] args = recvdMsg[0].split("\\|");

			if (args[0].equals("JOIN_REQ"))
			{
				String senderPort = args[1];

				String senderAVD = portNAvd.get(senderPort);

				String senderNodeID = "";
				try 
				{
					senderNodeID = genHash(senderAVD);
				} 
				catch (NoSuchAlgorithmException e) 
				{
					Log.e("ServerTask/onProgressUpdate/join/genHash", "NoSuchAlgorithm");
				}
				nodes.put(senderNodeID, senderAVD);
				for(Map.Entry<String,String> entry : nodes.entrySet())
				{
					String nodeID = entry.getKey();
					String AVDToSend = entry.getValue();
					if (nodes.size() == 1)
					{
						sNodes.put(nodeID, AVDToSend);
						pNodes.put(nodeID, AVDToSend);
					}
					else if (nodes.size() == 2)
					{
						if (nodes.higherKey(nodeID) != null)
						{
							sNodes.put(nodeID, nodes.get(nodes.higherKey(nodeID)));
							pNodes.put(nodeID, nodes.get(nodes.higherKey(nodeID)));
						}
						else if (nodes.lowerKey(nodeID) != null)
						{
							sNodes.put(nodeID, nodes.get(nodes.lowerKey(nodeID)));
							pNodes.put(nodeID, nodes.get(nodes.lowerKey(nodeID)));
						}
					}
					else if (nodes.size() > 2)
					{
						if (nodes.higherKey(nodeID) != null && nodes.lowerKey(nodeID) != null)
						{
							sNodes.put(nodeID, nodes.get(nodes.higherKey(nodeID)));
							pNodes.put(nodeID, nodes.get(nodes.lowerKey(nodeID)));
						}
						if (nodes.higherKey(nodeID) == null)
						{
							sNodes.put(nodeID, nodes.firstEntry().getValue());
							pNodes.put(nodeID, nodes.get(nodes.lowerKey(nodeID)));
						}
						if (nodes.lowerKey(nodeID) == null)
						{
							sNodes.put(nodeID, nodes.get(nodes.higherKey(nodeID)));
							pNodes.put(nodeID, nodes.lastEntry().getValue());
						}
					}
					noOfNodes = nodes.size();
					ClientTask clientTask = new ClientTask();		
					clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, "SEND_SP", 
							AVDToSend, 
							sNodes.get(nodeID) 
							+ ":" +
							pNodes.get(nodeID),
							String.valueOf(noOfNodes));
				}
			}
			else if (args[0].equals("SEND_SP"))
			{
				String msgRecvd = args[1];
				sNode = msgRecvd.split("\\:")[0];
				pNode = msgRecvd.split("\\:")[1];
				node = portNAvd.get(myPort);
				noOfNodes = Integer.parseInt(args[2]);
			}
			else if (args[0].equals("INS_REQ"))
			{
				String msg = args[1];
				String key = msg.split("\\:")[0];
				String value = msg.split("\\:")[1];
				ContentValues values = new ContentValues();
				values.put("key", key);
				values.put("value", value);
				insert(uri, values);
			}
			else if (args[0].equals("QUERY_SEL"))
			{
				String selection = args[1];
				String askingNode = args[2];
				queryHelper(uri, selection, askingNode, null);
			}
			else if (args[0].equals("QUERY_RES"))
			{
				if (args[2].equals(node))
				{
					String[] msg = args[1].split("\\:");
					MatrixCursor matrix = new MatrixCursor(new String[]{"key","value"});
					for (int i = 0; i < msg.length; i=i+2)
					{
						matrix.newRow().add(msg[i]).add(msg[i+1]);
					}
					finalCursor = matrix;
				}
				else
				{
					ClientTask clientTask = new ClientTask();		
					clientTask.executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, 
							"QUERY_RES", sNode, args[1], args[2]);
				}
			}
			else if (args[0].equals("QUERY_SEL_ALL"))
			{
				if (args[3].equals(node))
				{
					MatrixCursor matrix = new MatrixCursor(new String[]{"key","value"});
					if (args[2].length() != 0)
					{
						String[] msg = args[2].split("\\:");
						for (int i = 0; i < msg.length; i=i+2)
						{
							matrix.newRow().add(msg[i]).add(msg[i+1]);
						}
					}
					finalCursor = matrix;
					queryLock = false;
				}
				else
				{
					queryHelper(uri, args[1], args[3], args[2]); 
				}
			}
			else if (args[0].equals("DELETE_ALL"))
			{
				if (args[2].equals(node))
				{
					deleteLock = false;
				}
				else
				{
					String[] nodeToSend = new String[1];
					nodeToSend[0] = args[2];
					delete(uri, args[1], nodeToSend);
				}
			}
			return;
		}
	}
}
