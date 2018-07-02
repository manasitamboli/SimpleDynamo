/*REFERENCES:
		Assignment PA2A
		Assignment PA2B
		Assignment PA3
		https://developer.android.com/index.html
		https://docs.oracle.com/javase/tutorial/
		http://www.tutorialspoint.com/java/
		https://www.tutorialspoint.com/android/index.htm
		https://alvinalexander.com/java/jwarehouse/android-examples/samples/android-8/SearchableDictionary/src/com/example/android/searchabledict/DictionaryDatabase.java.shtml
		https://docs.oracle.com/javase/7/docs/api/java/util/HashMap.html
		https://www.java-examples.com/
		http://www.devmanuals.com/tutorials/java/corejava/index.html
		http://www.informit.com/articles/article.aspx?p=101766&seqNum=9
		https://docs.oracle.com/javase/specs/jls/se7/html/jls-4.html#jls-4.2.3
		https://developer.android.com/reference/android/database/MatrixCursor.html
		https://medium.com/@xabaras/creating-a-cursor-from-a-list-with-matrixcursor-ab71877ecf2c
		https://www.programcreek.com/java-api-examples/android.database.MatrixCursor
		http://blogs.innovationm.com/multiple-asynctask-in-android/
		https://developer.android.com/reference/android/os/AsyncTask.html
		https://dzone.com/articles/two-ways-convert-java-map
		https://docs.oracle.com/javase/7/docs/api/java/lang/StringBuilder.html
		https://docs.oracle.com/javase/tutorial/essential/concurrency/guardmeth.html
		https://www.programcreek.com/2009/02/notify-and-wait-example/
		https://www.cs.helsinki.fi/webfm_send/1041
		https://pdos.csail.mit.edu/papers/chord:tyan-meng.pdf
		https://pdos.csail.mit.edu/papers/chord:tburkard-meng.pdf
		https://www.w3resource.com/java-tutorial/
		https://www.101apps.co.za/articles/persisting-the-activity-instance-state.html
		https://www.101apps.co.za/articles/using-androids-sharedpreferences-to-save-data.html
		https://blog.lemberg.co.uk/how-use-sharedpreferences-application-configuration
		https://algorithms.tutorialhorizon.com/circular-linked-list-complete-implementation/
		http://tutorials.jenkov.com/java-util-concurrent/atomicinteger.html
		http://winterbe.com/posts/2015/05/22/java8-concurrency-tutorial-atomic-concurrent-map-examples/
		https://www.javamex.com/tutorials/synchronization_concurrency_7_atomic_integer_long.shtml
		https://www.callicoder.com/java-locks-and-atomic-variables-tutorial/
		https://stackoverflow.com/questions/10710193/how-to-preserve-insertion-order-in-hashmap
		https://docs.oracle.com/javase/8/docs/api/jaxva/util/HashMap.html
		https://developer.android.com/reference/java/util/HashMap
		https://docs.oracle.com/javase/8/docs/api/java/util/ArrayList.html
		https://chortle.ccsu.edu/java5/Notes/chap54/ch54_11.html
		https://www.java-made-easy.com/java-collections.html
		https://www.programcreek.com/2009/02/notify-and-wait-example/
		https://docs.oracle.com/javase/tutorial/essential/concurrency/guardmeth.html
		https://javarevisited.blogspot.com/2011/05/wait-notify-and-notifyall-in-java.html
		*/

package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.preference.PreferenceManager;
import android.telephony.TelephonyManager;
import android.util.Log;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OptionalDataException;
import java.io.StreamCorruptedException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple Dynamo provider.
 */
public class SimpleDynamoProvider extends ContentProvider {
    private static final String TAG = SimpleDynamoProvider.class.getSimpleName();
    private static LinkedHashMap<String, Integer> nodePortMap = new LinkedHashMap<String, Integer>() {{
        this.put("5554", 11108);
        this.put("5556", 11112);
        this.put("5558", 11116);
        this.put("5560", 11120);
        this.put("5562", 11124);
    }};
    private static final int SERVER_PORT = 10000;
    private static ArrayList<HashMap<String, String>> replicaList = new ArrayList<HashMap<String, String>>();
    private static HashMap<String, String> finalMap = new HashMap<String, String>();
    private static HashMap<String, Object> waitList = new HashMap<String, Object>();
    private static AtomicInteger atomicInt = new AtomicInteger(0);
    private static int replicateCount = 3;
    private static boolean nodeUp = false;
    private static int myPort;
    private static int size = 0;
    private static Node temp = null;

    private class Node implements Comparable<Node> {
        private int myPort;
        private String myNode;
        private Node prevNode;
        private Node nextNode;

        Node(int port, String nodeId, Node prev, Node next) {
            myPort = port;
            myNode = nodeId;
            prevNode = prev;
            nextNode = next;
        }

        @Override
        public int compareTo(Node temp) {
            return this.myNode.compareTo(temp.myNode);
        }
    }

    public int getNextPort(int port) {
        Node myNode = getNodeByPort(port);
        if (myNode != null) return myNode.nextNode.myPort;
        return 0;
    }

    public int getPrevPort(int port) {
        Node myNode = getNodeByPort(port);
        if (myNode != null) return myNode.prevNode.myPort;
        return 0;
    }

    private Node getNodeByPort(int port) {
        Node current = temp;
        for (int i = 0; i < size; i++) {
            if (current.myPort == port) {
                return current;
            }
            current = current.nextNode;
        }
        return null;
    }

    private Node getNodeByKey(String hashKey) {
        Node current = temp;
        for (int i = 0; i < size; i++) {
            if (hashKey.compareTo(current.myNode) <= 0) return current;
            current = current.nextNode;
        }
        return current;
    }

    @Override
    public boolean onCreate() {
        //super.onCreate(savedInstanceState);
        TelephonyManager tel = (TelephonyManager) this.getContext().getSystemService(
                Context.TELEPHONY_SERVICE);
        String portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
        myPort = Integer.parseInt(portStr) * 2;
        //Log.d("test", " port str : " + portStr);
        SharedPreferences sharedPref = PreferenceManager.getDefaultSharedPreferences(getContext());
		//Log.d("test", "shared pref : "+sharedPref.toString());
        SharedPreferences.Editor spEdit = sharedPref.edit();
        if (sharedPref.getBoolean("firstInstance", false)) {
            //Log.d("test", "entered shared pref recover true");
            nodeUp = true;
        } else {
            //Log.d("test", "entered shared pref recover false");
            spEdit.putBoolean("firstInstance", true);
            spEdit.apply();
        }
        for (int i = 0; i < replicateCount; i = i + 1) {
            replicaList.add(i, new HashMap<String, String>());
        }
        //Log.d("test", "replicaList on create : " + replicaList);
        //Reference : https://algorithms.tutorialhorizon.com/circular-linked-list-complete-implementation/
        try {
            for (Entry<String, Integer> pair : nodePortMap.entrySet()) {
                String key = pair.getKey();
                Integer value = pair.getValue();
                //Log.d("test", " test hash and port 1 : " + key + " - " + value);
                Node myNode = new Node(value.intValue(), genHash(key), null, null);
                if (size == 0) {
                    myNode.prevNode = myNode;
                    myNode.nextNode = myNode;
                    temp = myNode;
                    //Log.d("test", "mszie 0 : " + size + "-" + myNode);
                } else if (size == 1) {
                    myNode.prevNode = temp;
                    myNode.nextNode = temp;
                    temp.prevNode = myNode;
                    temp.nextNode = myNode;
                    //Log.d("test", "mszie 1 : " + size + "-" + myNode + "-" + temp);
                    if ((temp.compareTo(myNode) == 1) || (temp.compareTo(myNode) == 0)) {
                        //Log.d("test", "mszie 1 if compareto : " + size + "-" + myNode + "-" + temp);
                        temp = myNode;
                    }
                } else if (size > 1 && size < 5) {
                    Node current = temp;
                    for (int i = 0; i < size; i++) {
                        //Log.d("test", "size in fro loop : " + size);
                        if ((current.compareTo(myNode) == 1) || (current.compareTo(myNode) == 0)) {
                            myNode.prevNode = current.prevNode;
                            myNode.nextNode = current;
                            current.prevNode.nextNode = myNode;
                            current.prevNode = myNode;
                            //Log.d("test", "add 1 : " + size + "-" + myNode + "-" + current);
                            if (current.compareTo(temp) == 0)
                                temp = myNode;
                            break;
                        } else if ((current.nextNode).compareTo(temp) == 0) {
                            myNode.prevNode = current;
                            myNode.nextNode = temp;
                            current.nextNode = myNode;
                            temp.prevNode = myNode;
                            //Log.d("test", "add 2 : " + size + "-" + myNode + "-" + current + "-" + temp);
                            break;
                        } else {
                            current = current.nextNode;
                        }
                    }
                }
                size++;
                //Log.d("test", " test hash and port 2 : " + key + " - " + value);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        try {
            ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
            new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);
        } catch (IOException e) {
            Log.e(TAG, "Can't create a ServerSocket:\n" + e.getMessage());
            return false;
        }
        if (nodeUp) {
            String param1 = String.valueOf(getNextPort(myPort)) + "-" + "RECOVER" + "-" + "2" + "-" + null;
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param1);
            String param2 = String.valueOf(getNextPort(getNextPort(myPort))) + "-" + "RECOVER" + "-" + "2" + "-" + null;
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param2);
            String param3 = String.valueOf(getPrevPort(myPort)) + "-" + "RECOVER" + "-" + "1" + "-" + null;
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param3);
        }
        return true;
    }

    @Override
    public Uri insert(Uri uri, ContentValues values) {
        String key = values.getAsString("key");
        String value = values.getAsString("value");
        HashMap<String, String> map = new HashMap<String, String>();
        checkRecovery();
        map.put(key, value);
        //Log.d("test", "insert done in map : " + map);
        try {
            int insertPort = (getNodeByKey(genHash(key))).myPort;
            if (myPort == insertPort) {
                replicaList.get(0).putAll(map);
                String strMap = decomposeMap(map);
                String param = String.valueOf(getNextPort(insertPort)) + "-" + "INSERT" + "-" + "1" + "-" + strMap;
                //Log.d("test", "normal insert params if loop :"+param);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            } else {
                String strMap = decomposeMap(map);
                String param = String.valueOf(insertPort) + "-" + "INSERT" + "-" + "0" + "-" + strMap;
                //Log.d("test", "normal insert params else loop :"+param);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return uri;
    }

    @Override
    public Cursor query(Uri uri, String[] projection, String selection,
                        String[] selectionArgs, String sortOrder) {
        checkRecovery();
        //Log.d("test", "start normal query");
        HashMap<String, String> map = null;
        int queryPort = 0;
        if ((selection.equals("@")) || (selection.equals("*"))) {
            //Log.d("test", "start select query");
            return selectQuery(selection);
        }
        else {
            //Log.d("test", "start return query");
            return returnQuery(selection, map, queryPort);
        }
    }

    public synchronized Cursor selectQuery(String key) {
           String[] column = new String[]{"key", "value"};
           MatrixCursor cr = new MatrixCursor(column);
           if (key.equals("*")) {
               //Log.d("test", "* query started");
               ArrayList<Integer> ports = new ArrayList<Integer>();
               Node current = temp;
               for (int i = 0; i < size; i++) {
                   ports.add(current.myPort);
                   current = current.nextNode;
               }
               //Log.d("test", "map before claering : " + finalMap);
               finalMap.clear();
               for (int i = 0; i < ports.size(); i++) {
                   if (ports.get(i) == myPort) {
                       finalMap.putAll(replicaList.get(replicateCount - 1));
                       //atomicInt += atomicInt;
                       atomicInt.incrementAndGet();
                       //Log.d("test", "show map when myport : " + finalMap);
                   } else {
                       int count = replicateCount - 1;
                       String param = String.valueOf(ports.get(i)) + "-" + "GET_ALL" + "-" + String.valueOf(count) + "-" + null;
                       //Log.d("test", "* query params : " + param);
                       new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                   }
               }
               synchronized (atomicInt) {
                   try {
                       atomicInt.wait();
                   } catch (InterruptedException e) {
                       e.printStackTrace();
                   }
               }
               for (Entry<String, String> pair : finalMap.entrySet()) {
                   cr.addRow(new Object[]{pair.getKey(), pair.getValue()});
               }
           } else if (key.equals("@")) {
               //Log.d("test", "@ query started");
               for (int i = 0; i < replicateCount; i++) {
                   //Log.d("test", "show replicalist in @ query : " + replicaList);
                   for (Entry<String, String> pair : replicaList.get(i).entrySet()) {
                       cr.addRow(new Object[]{pair.getKey(), pair.getValue()});
                   }
               }
           }
           return cr;
    }

    public synchronized Cursor returnQuery(String key, HashMap<String, String> queryMap, int queryPort){
        //Log.d("test", "start return query");
        String[] column = new String[]{"key", "value"};
        MatrixCursor cr = new MatrixCursor(column);
        //Log.d("test", "return query map before clear : " + finalMap);
        finalMap.clear();
        //Log.d("test", "return query map after clear : " + finalMap);
        try {
            Node myNode = getNodeByKey(genHash(key));
            for (int i = 0; i < 2; i++) {
                myNode = myNode.nextNode;
            }
            queryPort = myNode.myPort;
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        //Log.d("test", "return query port : " + queryPort);
        if (myPort == queryPort) {
            //Log.d("test", "return query when port is queryPort");
            String value = replicaList.get(replicateCount - 1).get(key);
                    if (value.equals(null)) {
                        Object obj = waitList.get(key);
                        if (obj.equals(null)) {
                            obj = new Object();
                            waitList.put(key, obj);
                            //Log.d("test", "return query wait list after insert : " + waitList);
                        }
                        synchronized (obj) {
                            try {
                                obj.wait();
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                        value = replicaList.get(replicateCount - 1).get(key);
                        //Log.d("test", "return query value for key : " + key + "-" + value);
                    }
                    cr.addRow(new Object[]{key, value});
                } else {
                    queryMap = new HashMap<String, String>();
                    queryMap.put(key, "");
                    //Log.d("test", "querymap return query else loop : " + queryMap);
                    int count = replicateCount - 1;
                    String strMap = decomposeMap(queryMap);
                    String param = String.valueOf(queryPort) + "-" + "QUERY" + "-" + String.valueOf(count) + "-" + strMap;
            //Log.d("test", "return query else params : " + param);
            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                    synchronized (finalMap) {
                        try {
                            finalMap.wait();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    for (Entry<String, String> pair : finalMap.entrySet()) {
                        cr.addRow(new Object[]{pair.getKey(), pair.getValue()});
                    }
                }
            return cr;
    }

    @Override
    public int delete(Uri uri, String selection, String[] selectionArgs) {
        HashMap<String, String> map = new HashMap<String, String>();
        checkRecovery();
        map.put(selection, "");
        //Log.d("test", "del started show map : " + map);
        try {
            int delPort = (getNodeByKey(genHash(selection))).myPort;
            if (myPort == delPort) {
                replicaList.get(0).remove(selection);
                String strMap = decomposeMap(map);
                String param = String.valueOf(getNextPort(delPort)) + "-" + "DELETE" + "-" + "1" + "-" + strMap;
                //Log.d("test", "del if params : " + param);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            } else {
                String strMap = decomposeMap(map);
                String param = String.valueOf(delPort) + "-" + "DELETE" + "-" + "0" + "-" + strMap;
                //Log.d("test", "del else params : " + param);
                new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
            }
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public String getType(Uri uri) {
        return null;
    }

    @Override
    public int update(Uri uri, ContentValues values, String selection, String[] selectionArgs) {
        return 0;
    }

    @SuppressWarnings("resource")
    private String genHash(String input) throws NoSuchAlgorithmException {
        MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
        byte[] sha1Hash = sha1.digest(input.getBytes());
        Formatter formatter = new Formatter();
        for (byte b : sha1Hash) {
            formatter.format("%02x", b);
        }
        return formatter.toString();
    }

    private String decomposeMap(HashMap<String, String> map) {
        if (map.isEmpty()) {
            //Log.d("test", "null check entered in decompose : " + map);
            return null;
        } else {
            //Log.d("test", "null check entered in decompose : " + map);
            String[] keys = new String[map.size()];
            String[] values = new String[map.size()];
            int index = 0;
            for (Entry<String, String> myNode : map.entrySet()) {
                keys[index] = myNode.getKey();
                //Log.d("test", "decompose fn keys array : "+keys[index]);
                values[index] = myNode.getValue();
                //Log.d("test", "decompose fn values array : "+values[index]);
                //index++;
            }
            //Log.d("test", "decompose fn keys array 2 : "+keys[index]);
            //Log.d("test", "decompose fn values array 2 : "+values[index]);
            String sendMap = keys[index] + "," + values[index];
            return sendMap;
        }
    }

    public void checkRecovery() {
        if (nodeUp) {
            synchronized (atomicInt) {
                try {
                    atomicInt.wait();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    protected boolean handleFailure(String option, int port, int index, HashMap<String, String> map, boolean pickNext, boolean moveToNext) {
        Socket socket = null;
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        try {
            socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), port);
            socket.setSoTimeout(3000);
            if ((option.equals("INSERT") || option.equals("DELETE")) && index < replicateCount - 1)
                pickNext = true;
            Message msgReceived = new Message(option, index, pickNext, moveToNext, map);
            try {
                oos = new ObjectOutputStream(socket.getOutputStream());
                ///Log.d("test", "moveToNext val with client 1 : " + msgReceived.moveToNext);
                oos.writeObject(msgReceived);
            } catch (IOException e) {
                Log.e("test", "node fail 1");
                return false;
            }
            try {
                ois = new ObjectInputStream(socket.getInputStream());
                msgReceived = (Message) ois.readObject();
                //Log.d("test", "moveToNext val with client 2 : " + msgReceived.moveToNext);
            } catch (IOException e) {
                Log.e(TAG, "node fail 2");
                return false;
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                return false;
            }
            oos.flush();
            if (option.equals("QUERY")) {
                finalMap.putAll(msgReceived.map);
                synchronized (finalMap) {
                    finalMap.notify();
                }
            } else if (option.equals("GET_ALL")) {
                finalMap.putAll(msgReceived.map);
                if (atomicInt.incrementAndGet() == size) {
                    atomicInt.set(0);
                    synchronized (atomicInt) {
                        atomicInt.notify();
                    }
                }
            } else if (option.equals("RECOVER")) {
                if (port == getPrevPort(myPort)) {
                    replicaList.get(2).putAll(msgReceived.map);
                } else if (port == getNextPort(myPort)) {
                    replicaList.get(1).putAll(msgReceived.map);
                } else {
                    replicaList.get(0).putAll(msgReceived.map);
                }
                if (atomicInt.incrementAndGet() == replicateCount) {
                    nodeUp = false;
                    atomicInt.set(0);
                    synchronized (atomicInt) {
                        //Log.d("test", "release all locks");
                        atomicInt.notifyAll();
                    }
                }
            }
        } catch (UnknownHostException e) {
            Log.e(TAG, "ClientTask UnknownHostException");
            return false;
        } catch (SocketTimeoutException e) {
            Log.e(TAG, "ClientTask socket TimeOutException " + e.getLocalizedMessage());
            return false;
        } catch (SocketException e) {
            Log.e(TAG, "ClientTask socket Exception" + e.getLocalizedMessage());
            return false;
        } catch (IOException e) {
            Log.e(TAG, "IOException " + e.getLocalizedMessage());
            return false;
        }
        return true;
    }

    private class ServerTask extends AsyncTask<ServerSocket, String, Void> {

        @Override
        protected Void doInBackground(ServerSocket... sockets) {
            ServerSocket serverSocket = sockets[0];
            //DataInputStream dis = null;
            ObjectInputStream ois;
            ObjectOutputStream oos;
            Socket socket = null;
            Uri uri = null;
            try {
                while (true) {
                    socket = serverSocket.accept();
                    ois = new ObjectInputStream(socket.getInputStream());
                    oos = new ObjectOutputStream(socket.getOutputStream());
                    oos.flush();
                    Message msg = (Message) ois.readObject();
                    //Log.d("test", "moveToNext val server 0 : " + msg.moveToNext);
                    checkRecovery();
                    if (msg.option.equals("INSERT")) {
                        replicaList.get(msg.nodePosition).putAll(msg.map);
                        //Log.d("test", "inserting : " + msg.map.keySet().iterator().next() + " at " + msg.nodePosition);
                        if (waitList.size() > 0) {
                            String key = msg.map.keySet().iterator().next();
                            Object obj = waitList.get(key);
                            if (!(obj.equals(null))) {
                                waitList.remove(key);
                                synchronized (obj) {
                                    obj.notifyAll();
                                }
                            }
                        }
                        if (msg.pickNext) {
                            String strMap = decomposeMap(msg.map);
                            String param = String.valueOf(getNextPort(myPort)) + "-" + msg.option + "-" + String.valueOf(msg.nodePosition + 1) + "-" + strMap;
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                        }
                        if (msg.moveToNext) {
                            String strMap = decomposeMap(msg.map);
                            String param = String.valueOf(getPrevPort(myPort)) + "-" + "RETRY" + "-" + String.valueOf(msg.nodePosition - 1) + "-" + strMap;
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                        }
                        //Log.d("test", "moveToNext val server 1 : " + msg.moveToNext);
                        oos.writeObject(new Message("SUCCESS", 0, false, msg.moveToNext, null));
                    } else if (msg.option.equals("QUERY")) {
                        //Log.d("test", "key test iterator : "+ msg.map);
                        if (msg.map.keySet().iterator().hasNext()) {
                            String key = msg.map.keySet().iterator().next();
                            //Log.d("test", "key test iterator in loop : "+key);
                            String value = replicaList.get(msg.nodePosition).get(key);
                            if (value.equals(null)) {
                                Object obj = waitList.get(key);
                                if (obj.equals(null)) {
                                    obj = new Object();
                                    waitList.put(key, obj);
                                }
                                synchronized (obj) {
                                    obj.wait();
                                }
                                value = replicaList.get(msg.nodePosition).get(key);
                            }
                            msg.map.put(key, value);
                            oos.writeObject(msg);
                        }
                    } else if (msg.option.equals("GET_ALL")) {
                        msg.map = replicaList.get(msg.nodePosition);
                        oos.writeObject(msg);
                    } else if (msg.option.equals("DELETE")) {
                        String key = msg.map.keySet().iterator().next();
                        replicaList.get(msg.nodePosition).remove(key);
                        if (msg.pickNext) {
                            String strMap = decomposeMap(msg.map);
                            String param = String.valueOf(getNextPort(myPort)) + "-" + msg.option + "-" + String.valueOf(msg.nodePosition + 1) + "-" + strMap;
                            new ClientTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, param);
                        }
                        //Log.d("test", "moveToNext val server 3 : " + msg.moveToNext);
                        oos.writeObject(new Message("SUCCESS", 0, false, msg.moveToNext, null));
                    } else {
                        msg.map = replicaList.get(msg.nodePosition);
                        oos.writeObject(msg);
                    }
                    oos.flush();
                }
            } catch (UnknownHostException e) {
                Log.e(TAG, "ServerTask UnknownHostException");
            } catch (IOException e) {
                Log.e(TAG, "ServerTask socket IOException:\n" + e.getMessage());
            } catch (ClassNotFoundException e) {
                Log.e(TAG, "ServerTask ObjectInputStream ClassNotFoundException");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private Uri buildUri(String scheme, String authority) {
        Uri.Builder uriBuilder = new Uri.Builder();
        uriBuilder.authority(authority);
        uriBuilder.scheme(scheme);
        return uriBuilder.build();
    }

    private class ClientTask extends AsyncTask<String, Void, Void> {

        @Override
        protected Void doInBackground(String... msgs) {
            String[] msgToSend = msgs[0].trim().split("-");
            final int port = Integer.parseInt(msgToSend[0]);
            final String option = msgToSend[1];
            final int nodePosition = Integer.parseInt(msgToSend[2]);
            //Log.d(TAG, "client index in bytes :" + index);
            String temp = msgToSend[3];
            HashMap<String, String> map;
            //Log.d("test", "map in try client : "  + temp);
            if (temp.matches("null")) {
                map = null;
                if (option.equals("RETRY")) {
                    handleFailure("INSERT", port, nodePosition, map, false, false);
                } else if (!handleFailure(option, port, nodePosition, map, false, false)) {
                    Log.e("test", "node fail check");
                    if (option.equals("QUERY") || option.equals("GET_ALL")) {
                        try {
                            handleFailure(option, getPrevPort(port), nodePosition - 1, map, false, false);
                        } catch (Exception e) {
                            Log.e("test", "query fail");
                        }
                    } else if (option.equals("INSERT") || option.equals("DELETE")) {
                        if (nodePosition != replicateCount - 1) {
                            try {
                                handleFailure(option, getNextPort(port), nodePosition + 1, map, false, true);
                            } catch (Exception e) {
                                Log.e("test", "insert fail");
                            }
                        }
                    }
                }
            } else {
                //Log.d("test", "msg not null : " + msgToSend[3]);
                String[] keyval = temp.split(",");
                //Log.d("test", "keyval 0 : " +keyval[0]);
                String keysStr = keyval[0];
                if (keyval.length != 1) {
                    String valueStr = keyval[1];
                    Log.d("test", "keyval 1 : " + keyval[1]);
                    map = new HashMap<String, String>();
                    map.put(keysStr, valueStr);
                    //Log.d("test", "new map in client every time : " + map);
                } else {
                    map = new HashMap<String, String>();
                    map.put(keysStr, "");
                    //Log.d("test", "new map in client every time 2 : " + map);
                }
                //Log.d("test", "map is set to : " + map);
                if (option.equals("RETRY")) {
                    handleFailure("INSERT", port, nodePosition, map, false, false);
                } else if (!handleFailure(option, port, nodePosition, map, false, false)) {
                    Log.e("test", "node fail check");
                    if (option.equals("QUERY") || option.equals("GET_ALL")) {
                        try {
                            handleFailure(option, getPrevPort(port), nodePosition - 1, map, false, false);
                        } catch (Exception e) {
                            Log.e("test", "query fail");
                        }
                    } else if (option.equals("INSERT") || option.equals("DELETE")) {
                        if (nodePosition != replicateCount - 1) {
                            //Log.d("test", "from client to fail client insert ");
                            try {
                                handleFailure(option, getNextPort(port), nodePosition + 1, map, false, true);
                            } catch (Exception e) {
                                Log.e("test", "insert fail");
                            }
                        }
                    }
                }
            }
            return null;
        }

    }

}

