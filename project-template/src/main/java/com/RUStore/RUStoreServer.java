package com.RUStore;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;

public class RUStoreServer {
    private static HashMap<String, byte[]> objectStore = new HashMap<>();

    static class ClientHandler implements Runnable {
        private Socket clientSocket;
        private HashMap<String, byte[]> objectStore;

        public ClientHandler(Socket clientSocket, HashMap<String, byte[]> objectStore) {
            this.clientSocket = clientSocket;
            this.objectStore = objectStore;
        }

        @Override
		public void run() {
			try {
				// create DataInputStream and DataOutputStream for communication protocol
				DataInputStream in = new DataInputStream(clientSocket.getInputStream());
				DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

				while (true) {
					// read the command from the client
					String command = in.readUTF();

					// log the received command
					System.out.println("Received command: " + command);

					// based on the command, decide which operation to use
					if ("PUT".equals(command)) {
						handlePut(in, out);
					} else if ("GET".equals(command)) {
						handleGet(in, out);
					} else if ("REMOVE".equals(command)) {
						handleRemove(in, out);
					} else if ("LIST".equals(command)) {
						handleList(out);
					} else if ("DISCONNECT".equals(command)) {
						clientSocket.close();
						break;
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		private void handlePut(DataInputStream in, DataOutputStream out) throws IOException {
			// read the key from the client
			String key = in.readUTF();
		
			// read the data size from the client
			int dataSize = in.readInt();
		
			// read the actual data from the client
			byte[] data = new byte[dataSize];
			in.readFully(data);
		
			// store the data in the objectStore hashmap
			if (objectStore.containsKey(key)) {
				System.out.println("Key already exists. Sending response: 1");
				out.writeInt(1);  // key already exists
			} else {
				System.out.println("Storing data. Sending response: 0");
				objectStore.put(key, data);
				out.writeInt(0);  // data stored successfully
			}
		}

		private void handleGet(DataInputStream in, DataOutputStream out) throws IOException {
			System.out.println("Handling GET request");
	
			// read the key from the client
			String key = in.readUTF();
	
			if (objectStore.containsKey(key)) {
				byte[] data = objectStore.get(key);
	
				// send a success response to the client
				out.writeInt(0);
	
				// send the data size to the client
				out.writeInt(data.length);
	
				// send the actual data to the client
				out.write(data);
				out.flush();
	
			} else {
				// send a key not found response to the client
				out.writeInt(1);
				System.out.println("Key not found. Sending response: 1");
			}
		}

		private void handleRemove(DataInputStream in, DataOutputStream out) throws IOException {
			// read the key from the client
			String key = in.readUTF();
		
			// check if the key exists in the objectStore
			if (objectStore.containsKey(key)) {
				// remove the file associated with the key
				objectStore.remove(key);
				System.out.println("Removed object with key: " + key);
		
				// send a success response
				out.writeInt(0);
			} else {
				// key doesn't exist
				out.writeInt(1);
				System.out.println("Key doesn't exist. Sending response: 1");
			}
		}

		private void handleList(DataOutputStream out) throws IOException {
			// get the list of keys from the object store
			String[] keys = objectStore.keySet().toArray(new String[0]);
		
			// send the number of keys to the client
			out.writeInt(keys.length);
		
			// send each key to the client
			for (String key : keys) {
				out.writeUTF(key);
			}
		}		
    }
    public static void main(String[] args) {
        // check if port number is provided
        if (args.length != 1) {
            System.out.println("Usage: java RUStoreServer <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        ServerSocket serverSocket = null;

        try {
            // create a server socket
            serverSocket = new ServerSocket(port);
            System.out.println("Server started. Listening on port " + port);

            while (true) {
                // accept client connection
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected from " + clientSocket.getInetAddress());

                // start a new thread to handle the client
                Thread clientThread = new Thread(new ClientHandler(clientSocket, objectStore));
                clientThread.start();
            }
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        } finally {
            try {
                if (serverSocket != null) {
                    serverSocket.close();
                }
            } catch (Exception e) {
                System.out.println("Error closing server socket: " + e.getMessage());
            }
        }
    }
}
