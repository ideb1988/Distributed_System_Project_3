package edu.buffalo.cse.cse486586.simpledht;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import android.os.AsyncTask;
import android.util.Log;

public class ClientTask extends AsyncTask<String, Void, Void> implements Serializable
{
	// CLASS VARIABLES

	private static final long serialVersionUID = 1L;
	HashMap<String, String> avdNPort= new HashMap<String, String>();

	// CONSTRUCTOR

	public ClientTask()
	{
		this.avdNPort.put("5554", "11108");
		this.avdNPort.put("5556", "11112");
		this.avdNPort.put("5558", "11116");
		this.avdNPort.put("5560", "11120");
		this.avdNPort.put("5562", "11124");
	}

	// ASYNC METHOD

	protected Void doInBackground(String... args) 
	{
		if (args[0].equals("JOIN_REQ"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get("5554")));
				String msgToSend = args[0] + "|" + args[1] + "|" + args[2];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			}
		}
		else if (args[0].equals("SEND_SP"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get(args[1])));
				String msgToSend = args[0] + "|" + args[2] + "|" + args[3];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			}
		}
		else if (args[0].equals("INS_REQ"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get(args[1])));
				String msgToSend = args[0] + "|" + args[2] + "|" + args[3];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			}
		}
		else if (args[0].equals("QUERY_SEL"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get(args[1])));
				String msgToSend = args[0] + "|" + args[2] + "|" + args[3];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			} 
		}
		else if (args[0].equals("QUERY_RES"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get(args[1])));
				String msgToSend = args[0] + "|" + args[2] + "|" + args[3];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			} 
		}
		else if (args[0].equals("QUERY_SEL_ALL"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get(args[1])));
				String msgToSend = args[0] + "|" + args[2] + "|" + args[3] + "|" + args[4];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			} 
		}
		else if (args[0].equals("DELETE_ALL"))
		{
			try
			{				
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}),
						Integer.parseInt(avdNPort.get(args[1])));
				String msgToSend = args[0] + "|" + args[2] + "|" + args[3];
				ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
				out.writeObject(msgToSend);
				out.flush();
				out.close();
				socket.close();
			}
			catch (UnknownHostException e) 
			{
				Log.e("ClientTask", "UnknownHostException");
			} 
			catch (IOException e) 
			{
				Log.e("ClientTask", "socket IOException");
			} 
		}
		return null;
	}
}