package com.RUStore;

/* any necessary Java packages here */

import java.io.*;
import java.net.*;
import java.nio.file.Files;
import java.nio.file.Paths;

public class RUStoreClient {

	/* any necessary class members here */
	private Socket socket;
    private DataOutputStream out;
    private DataInputStream in;
    private String host;
    private int port;

	/**
	 * RUStoreClient Constructor, initializes default values
	 * for class members
	 *
	 * @param host	host url
	 * @param port	port number
	 */
	public RUStoreClient(String host, int port) {

		// Implement here
		this.host = host;
        this.port = port;
	}

	/**
	 * Opens a socket and establish a connection to the object store server
	 * running on a given host and port.
	 *
	 * @return		n/a, however throw an exception if any issues occur
	 */
	
	public void connect() throws IOException {
        socket = new Socket(host, port);
        out = new DataOutputStream(socket.getOutputStream());
        in = new DataInputStream(socket.getInputStream());
    }

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT be 
	 * overwritten
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param data	byte array representing arbitrary data object
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 */
	public int put(String key, byte[] data) {
		try {
			out.writeUTF("PUT");
			out.writeUTF(key);
			out.writeInt(data.length);
			out.write(data);
			out.flush(); 

			// Await response from server
			int response = in.readInt();

			if (response == 0) {  // Success
				return 0;
			} else if (response == 1) {  // Key already exists
				return 1;
			} else {
				throw new RuntimeException("Unexpected response from server during PUT operation");
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error during PUT operation", e);
		} 
	}
	
	

	/**
	 * Sends an arbitrary data object to the object store server. If an 
	 * object with the same key already exists, the object should NOT 
	 * be overwritten.
	 * 
	 * @param key	key to be used as the unique identifier for the object
	 * @param file_path	path of file data to transfer
	 * 
	 * @return		0 upon success
	 *        		1 if key already exists
	 *        		Throw an exception otherwise
	 */

	

	public int put(String key, String file_path) {
		try {
			byte[] data = Files.readAllBytes(Paths.get(file_path));
			return put(key, data);
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error reading from file during PUT operation", e);
		}
	}
	


		/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server.
	 * 
	 * @param key   key associated with the object
	 * 
	 * @return      object data as a byte array, null if key doesn't exist.
	 *              Throw an exception if any other issues occur.
	 */
	public byte[] get(String key) {
		try {
			// Send "GET" command
			out.writeUTF("GET");

			// Send the key
			out.writeUTF(key);

			// Await response from server
			int response = in.readInt();

			if (response == 0) {  // Success
				// Read the length of the data
				int dataLength = in.readInt();

				// Read the actual data
				byte[] data = new byte[dataLength];
				in.readFully(data);
				return data;
			} else if (response == 1) {  // Key doesn't exist
				return null;
			} else {
				throw new RuntimeException("Unexpected response from server during GET operation");
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error during GET operation", e);
		}
	}

	/**
	 * Downloads arbitrary data object associated with a given key
	 * from the object store server and places it in a file. 
	 * 
	 * @param key       key associated with the object
	 * @param file_path output file path
	 * 
	 * @return          0 upon success
	 *                  1 if key doesn't exist
	 *                  Throw an exception otherwise
	 */
	public int get(String key, String file_path) {
		try {
			byte[] data = get(key);

			if (data == null) {
				return 1;  // Key doesn't exist
			}

			// Write the data to the file
			Files.write(Paths.get(file_path), data);
			return 0;  // Success
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error writing to file during GET operation", e);
		}
	}

	

	/**
	 * Removes data object associated with a given key 
	 * from the object store server. Note: No need to download the data object, 
	 * simply invoke the object store server to remove object on server side
	 * 
	 * @param key	key associated with the object
	 * 
	 * @return		0 upon success
	 *        		1 if key doesn't exist
	 *        		Throw an exception otherwise
	 */
	public int remove(String key) {
		try {
			// Send "REMOVE" command
			out.writeUTF("REMOVE");
	
			// Send the key
			out.writeUTF(key);
	
			// Await response from server
			int response = in.readInt();
	
			if (response == 0) {  // Success
				return 0;
			} else if (response == 1) {  // Key doesn't exist
				return 1;
			} else {
				throw new RuntimeException("Unexpected response from server during REMOVE operation");
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error during REMOVE operation", e);
		}
	}
	

	/**
	 * Retrieves of list of object keys from the object store server
	 * 
	 * @return		List of keys as string array, null if there are no keys.
	 *        		Throw an exception if any other issues occur.
	 */
	public String[] list() {
		try {
			// Send "LIST" command
			out.writeUTF("LIST");
	
			// Await response from server for the number of keys
			int keyCount = in.readInt();
	
			if (keyCount == 0) {
				return null;
			}
	
			String[] keys = new String[keyCount];
			for (int i = 0; i < keyCount; i++) {
				keys[i] = in.readUTF();
			}
	
			return keys;
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error during LIST operation", e);
		}
	}
	

	/**
	 * Signals to server to close connection before closes 
	 * the client socket.
	 * 
	 * @return		n/a, however throw an exception if any issues occur
	 */
	public void disconnect() {
		try {
			if (!socket.isClosed()) {
				// Send "DISCONNECT" command
				out.writeUTF("DISCONNECT");
	
				// Close the streams and the socket
				in.close();
				out.close();
				socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException("Error during DISCONNECT operation", e);
		}
	}
	
	

}
