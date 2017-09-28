package org.talend.components.service.rest;

public class ServiceConstants {

    public static final String V0 = "v0";// initial release without versions so we use an empty one

    /** http content type to reflect one single jsonio property payload */
    public final static String JSONIO_CONTENT_TYPE = "application/jsonio+json;charset=UTF-8";

    /** http content type to reflect a set of jsonio properties payload */
    public final static String MULTPL_JSONIO_CONTENT_TYPE = "application/jsonios+json;charset=UTF-8";

    /** http content type to reflect one single uispec property payload */
    public final static String UI_SPEC_CONTENT_TYPE = "application/uispec+json;charset=UTF-8";

    /** http content type to reflect a set of uispec properties payload */
    public final static String MLTPL_UI_SPEC_CONTENT_TYPE = "application/uispecs+json;charset=UTF-8";
}
