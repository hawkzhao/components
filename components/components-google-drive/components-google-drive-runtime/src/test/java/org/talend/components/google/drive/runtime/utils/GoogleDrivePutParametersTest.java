package org.talend.components.google.drive.runtime.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

public class GoogleDrivePutParametersTest {

    GoogleDrivePutParameters parameters;

    @Before
    public void setUp() throws Exception {
        parameters = new GoogleDrivePutParameters("destination", "resource", true, "file");
    }

    @Test
    public void testGetDestinationFolderName() throws Exception {
        assertEquals("destination", parameters.getDestinationFolderName());
    }

    @Test
    public void testGetResourceName() throws Exception {
        assertEquals("resource", parameters.getResourceName());
    }

    @Test
    public void testIsOverwriteIfExist() throws Exception {
        assertTrue(parameters.isOverwriteIfExist());
    }

    @Test
    public void testGetFromLocalFilePath() throws Exception {
        assertEquals("file", parameters.getFromLocalFilePath());
    }

    @Test
    public void testGetFromBytes() throws Exception {
        GoogleDrivePutParameters p2 = new GoogleDrivePutParameters("destination", "resource", true, "byteContent".getBytes());
        assertEquals("byteContent", new String(p2.getFromBytes()));
    }

}
