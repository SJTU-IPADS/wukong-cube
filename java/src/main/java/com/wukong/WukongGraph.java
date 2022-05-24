package com.wukong;

public class WukongGraph
{
    static {
        System.loadLibrary("wukong_java");
    }

    // api
    public native void retrieveClusterInfo();
    public native String executeSparqlQuery(String query);

    private native long connectToServer(String address, int port);
    private native void disconnectToServer(long native_client_handle);

    public WukongGraph(String address, int port)
    {
        this.native_client_handle = connectToServer(address, port);
    }

    public void finalize()
    {
        disconnectToServer(this.native_client_handle);
    }

    private long native_client_handle = 0;

    // public static void main( String[] args )
    // {
    //     System.out.println( "Hello World!" );
    // }
}
