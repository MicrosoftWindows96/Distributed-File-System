import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

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
    private CountDownLatch storeAckLatch;
    private CountDownLatch removeAckLatch;
    private Queue<String> pendingRequests;
    private boolean rebalanceInProgress;

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
        this.pendingRequests = new LinkedBlockingQueue<>();
        this.rebalanceInProgress = false;
    }

    public void start() {
        try {
            ServerSocket serverSocket = new ServerSocket(cport);
            System.out.println("Controller started on port " + cport);

            // Schedule periodic rebalance operation
            rebalanceTimer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (!rebalanceInProgress) {
                        rebalanceInProgress = true;
                        rebalance();
                        rebalanceInProgress = false;
                        processQueuedRequests();
                    }
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

        // 1. Get list of files from each Dstore
        Map<Integer, List<String>> dstoreFiles = getDstoreFiles();

        // 2. Revise file allocation
        Map<String, Set<Integer>> newAllocation = reviseFileAllocation(dstoreFiles);

        // 3. Send rebalance instructions to each Dstore
        sendRebalanceInstructions(newAllocation);

        // 4. Wait for rebalance completion acknowledgement from Dstores
        waitForRebalanceCompletion();

        // ✓ Remove files from the index if no Dstore reports having them during the rebalance operation.
        removeOrphanedFiles(dstoreFiles);
    }

    private Map<Integer, List<String>> getDstoreFiles() {
        Map<Integer, List<String>> dstoreFiles = new HashMap<>();
        for (int dstorePort : dstorePorts) {
            try {
                Socket dstoreSocket = dstoreConnections.get(dstorePort);
                if (dstoreSocket != null && dstoreSocket.isConnected()) {
                    PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);
                    BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));

                    dstoreOut.println(Protocol.LIST_TOKEN);
                    String response = dstoreIn.readLine();
                    if (response != null) {
                        if (response.startsWith(Protocol.LIST_TOKEN)) {
                            String filesString = response.substring(Protocol.LIST_TOKEN.length()).trim();
                            if (!filesString.isEmpty()) {
                                String[] files = filesString.split(" ");
                                dstoreFiles.put(dstorePort, Arrays.asList(files));
                            } else {
                                dstoreFiles.put(dstorePort, new ArrayList<>());
                            }
                        } else {
                            System.err.println("Unexpected response from Dstore " + dstorePort + ": " + response);
                        }
                    } else {
                        System.err.println("Empty response from Dstore " + dstorePort);
                    }
                } else {
                    System.err.println("Dstore " + dstorePort + " is not connected");
                }
            } catch (IOException e) {
                System.err.println("Error getting file list from Dstore " + dstorePort + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
        return dstoreFiles;
    }

    private Map<String, Set<Integer>> reviseFileAllocation(Map<Integer, List<String>> dstoreFiles) {
        Map<String, Set<Integer>> newAllocation = new HashMap<>();

        // Create a list of all files across all Dstores
        List<String> allFiles = new ArrayList<>();
        for (List<String> files : dstoreFiles.values()) {
            allFiles.addAll(files);
        }

        // Remove duplicates
        Set<String> uniqueFiles = new HashSet<>(allFiles);

        // Calculate the expected number of files per Dstore
        int numDstores = dstorePorts.size();
        int numFiles = uniqueFiles.size();
        int filesPerDstore = (int) Math.ceil((double) numFiles * replicationFactor / numDstores);

        // Assign files to Dstores
        List<Integer> dstoreList = new ArrayList<>(dstorePorts);
        int dstoreIndex = 0;
        for (String file : uniqueFiles) {
            Set<Integer> dstores = new HashSet<>();
            for (int i = 0; i < replicationFactor; i++) {
                int dstorePort = dstoreList.get(dstoreIndex);
                dstores.add(dstorePort);
                dstoreIndex = (dstoreIndex + 1) % numDstores;
            }
            newAllocation.put(file, dstores);
        }

        return newAllocation;
    }

    private void sendRebalanceInstructions(Map<String, Set<Integer>> newAllocation) {
        for (int dstorePort : dstorePorts) {
            try {
                Socket dstoreSocket = dstoreConnections.get(dstorePort);
                PrintWriter dstoreOut = new PrintWriter(dstoreSocket.getOutputStream(), true);

                // Create rebalance instructions for the Dstore
                StringBuilder instructionsBuilder = new StringBuilder();
                instructionsBuilder.append(Protocol.REBALANCE_TOKEN).append(" ");

                // Files to send
                List<String> filesToSend = new ArrayList<>();
                for (Map.Entry<String, Set<Integer>> entry : newAllocation.entrySet()) {
                    String file = entry.getKey();
                    Set<Integer> dstores = entry.getValue();
                    if (dstores.contains(dstorePort) && !fileAllocation.getOrDefault(file, Collections.emptySet()).contains(dstorePort)) {
                        filesToSend.add(file);
                    }
                }
                instructionsBuilder.append(filesToSend.size()).append(" ");
                for (String file : filesToSend) {
                    instructionsBuilder.append(file).append(" ");
                    Set<Integer> dstores = newAllocation.get(file);
                    instructionsBuilder.append(dstores.size()).append(" ");
                    for (int port : dstores) {
                        instructionsBuilder.append(port).append(" ");
                    }
                }

                // Files to remove
                List<String> filesToRemove = new ArrayList<>();
                for (Map.Entry<String, Set<Integer>> entry : fileAllocation.entrySet()) {
                    String file = entry.getKey();
                    Set<Integer> dstores = entry.getValue();
                    if (dstores.contains(dstorePort) && !newAllocation.getOrDefault(file, Collections.emptySet()).contains(dstorePort)) {
                        filesToRemove.add(file);
                    }
                }
                instructionsBuilder.append(filesToRemove.size()).append(" ");
                for (String file : filesToRemove) {
                    instructionsBuilder.append(file).append(" ");
                }

                // Send rebalance instructions to the Dstore
                dstoreOut.println(instructionsBuilder.toString());
            } catch (IOException e) {
                System.err.println("Error sending rebalance instructions to Dstore " + dstorePort + ": " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void waitForRebalanceCompletion() {
        CountDownLatch latch = new CountDownLatch(dstorePorts.size());

        for (int dstorePort : dstorePorts) {
            new Thread(() -> {
                try {
                    Socket dstoreSocket = dstoreConnections.get(dstorePort);
                    BufferedReader dstoreIn = new BufferedReader(new InputStreamReader(dstoreSocket.getInputStream()));

                    String response = dstoreIn.readLine();
                    if (response != null && response.equals(Protocol.REBALANCE_COMPLETE_TOKEN)) {
                        System.out.println("Received REBALANCE_COMPLETE from Dstore " + dstorePort);
                    } else {
                        System.err.println("Invalid response from Dstore " + dstorePort + " during rebalance: " + response);
                    }
                } catch (IOException e) {
                    System.err.println("Error waiting for rebalance completion from Dstore " + dstorePort + ": " + e.getMessage());
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            }).start();
        }

        try {
            latch.await();
            System.out.println("Rebalance completed");
        } catch (InterruptedException e) {
            System.err.println("Interrupted while waiting for rebalance completion: " + e.getMessage());
            e.printStackTrace();
        }
    }


    private void removeOrphanedFiles(Map<Integer, List<String>> dstoreFiles) {
        Set<String> reportedFiles = new HashSet<>();
        for (List<String> files : dstoreFiles.values()) {
            reportedFiles.addAll(files);
        }

        Set<String> orphanedFiles = new HashSet<>(fileAllocation.keySet());
        orphanedFiles.removeAll(reportedFiles);

        for (String file : orphanedFiles) {
            fileAllocation.remove(file);
            fileSizes.remove(file);
            fileStates.remove(file);
            System.out.println("Removed orphaned file from index: " + file);
        }
    }

    private void processQueuedRequests() {
        while (!pendingRequests.isEmpty()) {
            String request = pendingRequests.poll();
            String[] parts = request.split(" ");
            String command = parts[0];

            switch (command) {
                case Protocol.STORE_TOKEN:
                    handleStoreRequest(parts, new PrintWriter(System.out));
                    break;
                case Protocol.LOAD_TOKEN:
                    handleLoadRequest(parts, new PrintWriter(System.out));
                    break;
                case Protocol.REMOVE_TOKEN:
                    handleRemoveRequest(parts, new PrintWriter(System.out));
                    break;
                case Protocol.LIST_TOKEN:
                    handleListRequest(new PrintWriter(System.out));
                    break;
                case Protocol.JOIN_TOKEN:
                    handleJoinRequest(parts);
                    break;
                default:
                    System.out.println("Unknown command in queued request: " + command);
            }
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

                String request = in.readLine();
                System.out.println("Received request from client: " + request);

                if (request == null) {
                    System.out.println("Empty request received from client");
                    return;
                }

                if (rebalanceInProgress) {
                    // ✓ Queue client requests during the rebalance operation and serve them once the rebalance is completed.
                    pendingRequests.offer(request);
                    System.out.println("Queued request during rebalance: " + request);
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
                        // ✓ Queue Dstore JOIN requests during the rebalance operation and serve them once the rebalance is completed.
                        if (rebalanceInProgress) {
                            pendingRequests.offer(request);
                            System.out.println("Queued JOIN request during rebalance: " + request);
                        } else {
                            handleJoinRequest(parts);
                        }
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

        // Retry connecting to the Dstore with a delay
        boolean connected = false;
        int retryCount = 0;
        while (!connected && retryCount < 3) {
            try {
                Socket dstoreSocket = new Socket("localhost", dstorePort);
                dstoreConnections.put(dstorePort, dstoreSocket);
                System.out.println("Dstore " + dstorePort + " joined the system");
                connected = true;
            } catch (IOException e) {
                System.err.println("Error connecting to Dstore " + dstorePort + ": " + e.getMessage());
                e.printStackTrace();
                retryCount++;
                try {
                    Thread.sleep(1000); // Delay before retrying
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }

        if (!connected) {
            System.err.println("Failed to connect to Dstore " + dstorePort + " after " + retryCount + " attempts");
            dstorePorts.remove(dstorePort);
        } else {
            if (dstorePorts.size() >= replicationFactor) {
                rebalance();
            }
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