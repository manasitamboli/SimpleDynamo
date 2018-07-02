package edu.buffalo.cse.cse486586.simpledynamo;

import java.util.HashMap;
import java.io.Serializable;

/**
 * Created by manasitamboli on 4/22/18.
 */

public class Message implements Serializable {
    public String option;
    public int nodePosition;
    public boolean pickNext;
    public boolean moveToNext;
    public HashMap<String, String> map;

    public Message(String opt, int pos, boolean next, boolean mov, HashMap<String, String> map) {
        this.option = opt;
        this.nodePosition = pos;
        this.pickNext = next;
        this.moveToNext = mov;
        this.map = map;
    }
}