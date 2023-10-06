package com.RUStore;

/* any necessary Java packages here */
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;


public class RUStoreServer {
	private static HashMap<String, byte[]> objectStore = new HashMap<>();

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

				// TODO: Handle client requests in a separate thread
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
