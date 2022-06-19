package com.wukong;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class WukongGraphTest extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public WukongGraphTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( WukongGraphTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        WukongGraph wukong_client = new WukongGraph("127.0.0.1", 6577);
        wukong_client.retrieveClusterInfo();
        wukong_client.executeSparqlQuery("");
        assertTrue( true );
    }
}
