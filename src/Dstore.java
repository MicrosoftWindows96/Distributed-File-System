import java.io.*;
import java.net.*;
import java.util.*;

public class Dstore {
    private int port;
    private int cport;
    private int timeout;
    private String fileFolder;
    private Socket controllerSocket;
    private PrintWriter controllerOut;
    private BufferedReader controllerIn;

    public Dstore(int port, int cport, int timeout, String fileFolder) {
        this.port = port;
        this.cport = cport;
        this.timeout = timeout;
        this.fileFolder = fileFolder;
    }

    public void start() {
        try {
            controllerSocket = new Socket("localhost", cport);
            controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
            controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

            controllerOut.println(Protocol.JOIN_TOKEN + " " + port);

            ServerSocket serverSocket = new ServerSocket(port);
            System.out.println("Dstore started on port " + port);

            while (true) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    new Thread(new ClientHandler(clientSocket)).start();
                } catch (IOException e) {
                    System.err.println("Error accepting client connection: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (ConnectException e) {
            System.err.println("Failed to connect to the Controller: " + e.getMessage());
            e.printStackTrace();
            // Handle the failure appropriately, e.g., retry the connection or terminate the Dstore
        } catch (IOException e) {
            System.err.println("Error starting the Dstore: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private class ClientHandler implements Runnable {
        private Socket clientSocket;

        public ClientHandler(Socket clientSocket) {
            this.clientSocket = clientSocket;
        }

        @Override
        public void run() {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
                InputStream inputStream = clientSocket.getInputStream();
                OutputStream outputStream = clientSocket.getOutputStream();

                String request = in.readLine();
                if (request == null) {
                    System.out.println("Empty request received from client");
                    return;
                }

                String[] parts = request.split(" ");
                String command = parts[0];

                switch (command) {
                    case Protocol.STORE_TOKEN:
                        handleStoreRequest(parts, out, inputStream);
                        break;
                    case Protocol.LOAD_DATA_TOKEN:
                        handleLoadDataRequest(parts, outputStream);
                        break;
                    case Protocol.REMOVE_TOKEN:
                        handleRemoveRequest(parts);
                        break;
                    case Protocol.LIST_TOKEN:
                        handleListRequest(out);
                        break;
                    case Protocol.REBALANCE_STORE_TOKEN:
                        handleRebalanceStoreRequest(parts, out, inputStream);
                        break;
                    default:
                        System.out.println("Unknown command: " + command);
                }
            } catch (IOException e) {
                System.err.println("Error handling client request: " + e.getMessage());
                e.printStackTrace();
            } finally {
                try {
                    clientSocket.close();
                    System.out.println("Client connection closed: " + clientSocket);
                } catch (IOException e) {
                    System.err.println("Error closing client connection: " + e.getMessage());
                    e.printStackTrace();
                }
            }
        }

        private void handleStoreRequest(String[] parts, PrintWriter out, InputStream inputStream) {
            if (parts.length != 3) {
                System.out.println("Invalid store request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            out.println(Protocol.ACK_TOKEN);

            try {
                byte[] fileData = inputStream.readNBytes(filesize);
                FileOutputStream fileOutputStream = new FileOutputStream(fileFolder + File.separator + filename);
                fileOutputStream.write(fileData);
                fileOutputStream.close();

                controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
                System.out.println("Stored file: " + filename);
            } catch (IOException e) {
                System.err.println("Error storing file " + filename + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void handleLoadDataRequest(String[] parts, OutputStream outputStream) {
            if (parts.length != 2) {
                System.out.println("Invalid load data request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];
            File file = new File(fileFolder + File.separator + filename);

            if (!file.exists()) {
                System.out.println("File does not exist: " + filename);
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                    e.printStackTrace();
                }
                return;
            }

            try {
                FileInputStream fileInputStream = new FileInputStream(file);
                byte[] fileData = fileInputStream.readAllBytes();
                outputStream.write(fileData);
                fileInputStream.close();
                System.out.println("Loaded file: " + filename);
            } catch (IOException e) {
                System.err.println("Error loading file " + filename + ": " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void handleRemoveRequest(String[] parts) {
            if (parts.length != 2) {
                System.out.println("Invalid remove request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];
            File file = new File(fileFolder + File.separator + filename);

            if (file.exists()) {
                if (file.delete()) {
                    controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
                    System.out.println("Removed file: " + filename);
                } else {
                    System.err.println("Failed to remove file: " + filename);
                }
            } else {
                controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
                System.out.println("File does not exist: " + filename);
            }
        }

        private void handleListRequest(PrintWriter out) {
            File folder = new File(fileFolder);
            File[] files = folder.listFiles();

            StringBuilder response = new StringBuilder(Protocol.LIST_TOKEN);
            if (files != null) {
                for (File file : files) {
                    response.append(" ").append(file.getName());
                }
            }

            out.println(response.toString());
            System.out.println("Sent file list to client");
        }

        private void handleRebalanceStoreRequest(String[] parts, PrintWriter out, InputStream inputStream) {
            if (parts.length != 3) {
                System.out.println("Invalid rebalance store request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            out.println(Protocol.ACK_TOKEN);

            try {
                byte[] fileData = inputStream.readNBytes(filesize);
                FileOutputStream fileOutputStream = new FileOutputStream(fileFolder + File.separator + filename);
                fileOutputStream.write(fileData);
                fileOutputStream.close();
                System.out.println("Stored file during rebalance: " + filename);
            } catch (IOException e) {
                System.err.println("Error storing file during rebalance " + filename + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void sendRebalanceComplete() {
        controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
        System.out.println("Sent rebalance complete to Controller");
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Dstore <port> <cport> <timeout> <file_folder>");
            return;
        }

        int port = Integer.parseInt(args[0]);
        int cport = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        String fileFolder = args[3];

        Dstore dstore = new Dstore(port, cport, timeout, fileFolder);
        dstore.start();
    }
}