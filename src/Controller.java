import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Controller {
    private int cport;
    private int replicationFactor;
    private int timeout;
    private int rebalancePeriod;
    private Map<String, Set<Integer>> fileAllocation;
    private Map<String, Integer> fileSizes;
    private Map<String, String> fileStates;
    private Set<Integer> dstorePorts;
    private Map<Integer, Socket> dstoreConnections;
    private Timer rebalanceTimer;

    public Controller(int cport, int replicationFactor, int timeout, int rebalancePeriod) {
        this.cport = cport;
        this.replicationFactor = replicationFactor;
        this.timeout = timeout;
        this.rebalancePeriod = rebalancePeriod;
        this.fileAllocation = new HashMap<>();
        this.fileSizes = new HashMap<>();
        this.fileStates = new HashMap<>();
        this.dstorePorts = new HashSet<>();
        this.dstoreConnections = new HashMap<>();
        this.rebalanceTimer = new Timer();
    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(cport);
            System.out.println("Controller started on port " + cport);

            // Schedule periodic rebalance operation
            rebalanceTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    rebalance();
                }
            }, rebalancePeriod * 1000, rebalancePeriod * 1000);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                System.out.println("New client connected: " + clientSocket);
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Error starting the Controller: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void rebalance() {
        if (dstorePorts.size() < replicationFactor) {
            System.out.println("Not enough Dstores to perform rebalance");
            return;
        }

        // TODO: Implement rebalance logic
        System.out.println("Performing rebalance operation");
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

                String request = in.readLine();
                System.out.println("Received request from client: " + request);

                if (request == null) {
                    System.out.println("Empty request received from client");
                    return;
                }

                String[] parts = request.split(" ");
                String command = parts[0];

                switch (command) {
                    case Protocol.STORE_TOKEN:
                        handleStoreRequest(parts, out);
                        break;
                    case Protocol.LOAD_TOKEN:
                        handleLoadRequest(parts, out);
                        break;
                    case Protocol.REMOVE_TOKEN:
                        handleRemoveRequest(parts, out);
                        break;
                    case Protocol.LIST_TOKEN:
                        handleListRequest(out);
                        break;
                    case Protocol.JOIN_TOKEN:
                        handleJoinRequest(parts);
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

        private void handleStoreRequest(String[] parts, PrintWriter out) {
            if (parts.length != 3) {
                System.out.println("Invalid store request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];
            int filesize = Integer.parseInt(parts[2]);

            if (dstorePorts.size() < replicationFactor) {
                out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
                System.out.println("Not enough Dstores to handle store request for file: " + filename);
                return;
            }

            if (fileStates.containsKey(filename)) {
                out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
                System.out.println("File already exists: " + filename);
                return;
            }

            fileStates.put(filename, "store_in_progress");
            System.out.println("Storing file: " + filename);

            List<Integer> selectedDstores = selectDstores();
            StringBuilder storeToMessage = new StringBuilder(Protocol.STORE_TO_TOKEN);
            for (int dstorePort : selectedDstores) {
                storeToMessage.append(" ").append(dstorePort);
            }
            out.println(storeToMessage.toString());
            System.out.println("Sent STORE_TO message to client: " + storeToMessage);

            fileSizes.put(filename, filesize);
            fileAllocation.put(filename, new HashSet<>(selectedDstores));

            startStoreAckTimeout(filename);
        }

        private void handleLoadRequest(String[] parts, PrintWriter out) {
            if (parts.length != 2) {
                System.out.println("Invalid load request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];

            if (!fileStates.containsKey(filename) || !fileStates.get(filename).equals("store_complete")) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                System.out.println("File does not exist: " + filename);
                return;
            }

            int dstorePort = selectDstoreForLoad(filename);
            int filesize = fileSizes.get(filename);
            out.println(Protocol.LOAD_FROM_TOKEN + " " + dstorePort + " " + filesize);
            System.out.println("Sent LOAD_FROM message to client for file: " + filename);
        }

        private void handleRemoveRequest(String[] parts, PrintWriter out) {
            if (parts.length != 2) {
                System.out.println("Invalid remove request: " + Arrays.toString(parts));
                return;
            }

            String filename = parts[1];

            if (!fileStates.containsKey(filename)) {
                out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
                System.out.println("File does not exist: " + filename);
                return;
            }

            fileStates.put(filename, "remove_in_progress");
            System.out.println("Removing file: " + filename);

            Set<Integer> dstorePorts = fileAllocation.get(filename);
            for (int dstorePort : dstorePorts) {
                Socket dstoreSocket = dstoreConnections.get(dstorePort);
                if (dstoreSocket != null) {
                    try {
                        PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                        dstoreOut.println(Protocol.REMOVE_TOKEN + " " + filename);
                        System.out.println("Sent REMOVE message to Dstore: " + dstorePort);
                    } catch (IOException e) {
                        System.err.println("Error sending remove request to Dstore " + dstorePort + ": " + e.getMessage());
                        e.printStackTrace();
                    }
                }
            }

            startRemoveAckTimeout(filename);
        }

        private void handleListRequest(PrintWriter out) {
            List<String> completeFiles = new ArrayList<>();
            for (Map.Entry<String, String> entry : fileStates.entrySet()) {
                if (entry.getValue().equals("store_complete")) {
                    completeFiles.add(entry.getKey());
                }
            }

            if (completeFiles.isEmpty()) {
                out.println(Protocol.LIST_TOKEN);
            } else {
                StringBuilder listMessage = new StringBuilder(Protocol.LIST_TOKEN);
                for (String filename : completeFiles) {
                    listMessage.append(" ").append(filename);
                }
                out.println(listMessage.toString());
            }
            System.out.println("Sent LIST response to client");
        }

        private void handleJoinRequest(String[] parts) {
            if (parts.length != 2) {
                System.out.println("Invalid join request: " + Arrays.toString(parts));
                return;
            }

            int dstorePort = Integer.parseInt(parts[1]);
            dstorePorts.add(dstorePort);

            try {
                Socket dstoreSocket = new Socket("localhost", dstorePort);
                dstoreConnections.put(dstorePort, dstoreSocket);
                System.out.println("Dstore " + dstorePort + " joined the system");
            } catch (IOException e) {
                System.err.println("Error connecting to Dstore " + dstorePort + ": " + e.getMessage());
                e.printStackTrace();
            }

            if (dstorePorts.size() >= replicationFactor) {
                rebalance();
            }
        }

        private List<Integer> selectDstores() {
            List<Integer> selectedDstores = new ArrayList<>(dstorePorts);
            Collections.shuffle(selectedDstores);
            return selectedDstores.subList(0, replicationFactor);
        }

        private int selectDstoreForLoad(String filename) {
            Set<Integer> dstorePorts = fileAllocation.get(filename);
            return dstorePorts.iterator().next();
        }

        private void startStoreAckTimeout(String filename) {
            CountDownLatch latch = new CountDownLatch(replicationFactor);
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (latch.getCount() > 0) {
                        fileStates.remove(filename);
                        fileSizes.remove(filename);
                        fileAllocation.remove(filename);
                        System.out.println("Store operation timed out for file: " + filename);
                    } else {
                        fileStates.put(filename, "store_complete");
                        System.out.println("Store operation completed for file: " + filename);
                    }
                }
            }, timeout);

            for (int i = 0; i < replicationFactor; i++) {
                new Thread(() -> {
                    try {
                        // Wait for STORE_ACK from Dstore
                        // ...
                        latch.countDown();
                    } catch (Exception e) {
                        System.err.println("Error waiting for STORE_ACK: " + e.getMessage());
                        e.printStackTrace();
                    }
                }).start();
            }
        }

        private void startRemoveAckTimeout(String filename) {
            CountDownLatch latch = new CountDownLatch(replicationFactor);
            Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    if (latch.getCount() > 0) {
                        System.out.println("Remove operation timed out for file: " + filename);
                    } else {
                        fileStates.remove(filename);
                        fileSizes.remove(filename);
                        fileAllocation.remove(filename);
                        System.out.println("Remove operation completed for file: " + filename);
                    }
                }
            }, timeout);

            for (int i = 0; i < replicationFactor; i++) {
                new Thread(() -> {
                    try {
                        // Wait for REMOVE_ACK from Dstore
                        // ...
                        latch.countDown();
                    } catch (Exception e) {
                        System.err.println("Error waiting for REMOVE_ACK: " + e.getMessage());
                        e.printStackTrace();
                    }
                }).start();
            }
        }
    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Usage: java Controller <cport> <replication_factor> <timeout> <rebalance_period>");
            return;
        }

        int cport = Integer.parseInt(args[0]);
        int replicationFactor = Integer.parseInt(args[1]);
        int timeout = Integer.parseInt(args[2]);
        int rebalancePeriod = Integer.parseInt(args[3]);

        Controller controller = new Controller(cport, replicationFactor, timeout, rebalancePeriod);
        controller.start();
    }
}