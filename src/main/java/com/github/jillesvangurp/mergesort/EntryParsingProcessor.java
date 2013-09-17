package com.github.jillesvangurp.mergesort;

import java.util.Map.Entry;

import com.github.jillesvangurp.common.ImmutableEntry;
import com.jillesvangurp.iterables.Processor;

public class EntryParsingProcessor implements Processor<String, Entry<String,String>> {

    @Override
    public Entry<String, String> process(String line) {
        int idx = line.indexOf(';');
        if(idx < 0)
            throw new IllegalStateException("line does not contain semicolon! " + line);
        
        final String key=line.substring(0,idx);
        final String value=line.substring(idx+1);
        return new ImmutableEntry<String, String>(key, value);
    }
}