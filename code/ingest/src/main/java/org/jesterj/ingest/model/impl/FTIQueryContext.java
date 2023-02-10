package org.jesterj.ingest.model.impl;

import java.util.List;

public class FTIQueryContext {

    private final List<String> sentAlready;

    public FTIQueryContext(List<String> sentAlready) {
        this.sentAlready = sentAlready;
    }


    public List<String> getSentAlready() {
        return sentAlready;
    }
}
