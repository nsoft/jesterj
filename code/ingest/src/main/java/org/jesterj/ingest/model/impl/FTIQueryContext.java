package org.jesterj.ingest.model.impl;

import java.util.Set;

public class FTIQueryContext {

    private final Set<String> sentAlready;

    public FTIQueryContext(Set<String> sentAlready) {
        this.sentAlready = sentAlready;
    }


    public Set<String> getSentAlready() {
        return sentAlready;
    }
}
