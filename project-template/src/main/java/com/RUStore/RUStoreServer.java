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
				// Create DataInputStream and DataOutputStream for communication
				DataInputStream in = new DataInputStream(clientSocket.getInputStream());
				DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

				while (true) {
					// Read the command from the client
					String command = in.readUTF();

					// Log the received command
					System.out.println("Received command: " + command);

					// Based on the command, decide which handler method to invoke
					if ("PUT".equals(command)) {
						handlePut(in, out);
					} else if ("GET".equals(command)) { // Handle GET command
						handleGet(in, out);
					} else if ("REMOVE".equals(command)) {
						handleRemove(in, out);
					} else if ("LIST".equals(command)) {
						handleList(out);
					} else if ("DISCONNECT".equals(command)) {
						// Close the client socket when requested
						clientSocket.close();
						break; // Exit the loop
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}


		private void handlePut(DataInputStream in, DataOutputStream out) throws IOException {
			// Read the key from the client
			String key = in.readUTF();
		
			// Read the data size from the client
			int dataSize = in.readInt();
		
			// Read the data from the client
			byte[] data = new byte[dataSize];
			in.readFully(data);
		
			// Store the data in the objectStore HashMap
			if (objectStore.containsKey(key)) {
				System.out.println("Key already exists. Sending response: 1");
				out.writeInt(1);  // Key already exists
			} else {
				System.out.println("Storing data. Sending response: 0");
				objectStore.put(key, data);
				out.writeInt(0);  // Data stored successfully
			}
		}

		private void handleGet(DataInputStream in, DataOutputStream out) throws IOException {
			System.out.println("Handling GET request");
	
			// Read the key from the client
			String key = in.readUTF();
	
			if (objectStore.containsKey(key)) {
				byte[] data = objectStore.get(key);
	
				// Send a success response (0) to the client
				out.writeInt(0);
	
				// Send the data size to the client
				out.writeInt(data.length);
	
				// Send the data to the client
				out.write(data);
				out.flush();
	
			} else {
				// Send a key not found response (1) to the client
				out.writeInt(1);
				System.out.println("Key not found. Sending response: 1");
			}
		}

		private void handleRemove(DataInputStream in, DataOutputStream out) throws IOException {
			// Read the key from the client
			String key = in.readUTF();
		
			// Check if the key exists in the object store
			if (objectStore.containsKey(key)) {
				// Remove the file associated with the key
				objectStore.remove(key);
				System.out.println("Removed object with key: " + key);
		
				// Send a success response
				out.writeInt(0);  // Successful removal
			} else {
				// Key doesn't exist; send a response indicating that
				out.writeInt(1);  // Key doesn't exist
				System.out.println("Key doesn't exist. Sending response: 1");
			}
		}

		private void handleList(DataOutputStream out) throws IOException {
			// Get the list of keys from the object store
			String[] keys = objectStore.keySet().toArray(new String[0]);
		
			// Send the number of keys to the client
			out.writeInt(keys.length);
		
			// Send each key to the client
			for (String key : keys) {
				out.writeUTF(key);
			}
		}
		
		

				
    }

	

    public static void main(String[] args) {
        // Check if port number is provided
        if (args.length != 1) {
            System.out.println("Usage: java RUStoreServer <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[0]);
        ServerSocket serverSocket = null;

        try {
            // Create a server socket
            serverSocket = new ServerSocket(port);
            System.out.println("Server started. Listening on port " + port);

            while (true) {
                // Accept client connections
                Socket clientSocket = serverSocket.accept();
                System.out.println("Client connected from " + clientSocket.getInetAddress());

                // Start a new thread to handle the client's request
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
