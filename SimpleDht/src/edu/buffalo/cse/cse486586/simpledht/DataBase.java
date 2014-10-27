package edu.buffalo.cse.cse486586.simpledht;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.util.Log;

public class DataBase extends SQLiteOpenHelper
{
	public static final int db_ver = 1;
	public static final String db_name = "simpleDHT.db";
	public String db_table;
	Context _context;

	public DataBase(Context _context , String uri)
	{
		super(_context, db_name, null, db_ver);
		this.db_table = "[" +uri.toString()+"]" ;
		this._context = _context;
	}

	@Override
	public void onCreate(SQLiteDatabase mydb) 
	{
		String db_create_table = "CREATE TABLE " 
									+ db_table
									+ " (key TEXT PRIMARY KEY, " 
									+ "value TEXT NOT NULL);";
		
		mydb.execSQL(db_create_table);
		Log.v(db_name ,"Table Created");
	}

	@Override
	public void onUpgrade(SQLiteDatabase mydb, int db_ver1, int db_ver2) 
	{
		Log.v(db_name ,"Table Upgraded");
	}
}
