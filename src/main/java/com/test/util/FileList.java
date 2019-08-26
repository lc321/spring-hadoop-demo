package com.test.util;

public class FileList {

    String name;

    String folder;

    long size;

    public FileList() {
    }

    public FileList(String name, String folder, long size) {
        this.name = name;
        this.folder = folder;
        this.size = size;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getFolder() {
        return folder;
    }

    public void setFolder(String folder) {
        this.folder = folder;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }
}
