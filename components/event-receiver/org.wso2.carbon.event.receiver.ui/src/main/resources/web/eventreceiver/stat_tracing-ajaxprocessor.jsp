<%--
  ~ Copyright (c) 2005 - 2014, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
  ~
  ~ Licensed under the Apache License, Version 2.0 (the "License"); you may not
  ~ use this file except in compliance with the License. You may obtain a copy
  ~ of the License at
  ~
  ~ http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing, software distributed
  ~ under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
  ~ CONDITIONS OF ANY KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations under the License.
  --%>
<%@ page language="java" contentType="text/html; charset=ISO-8859-1" pageEncoding="ISO-8859-1" %>

<%@ page import="org.wso2.carbon.event.receiver.stub.EventReceiverAdminServiceStub" %>
<%@ page
        import="org.wso2.carbon.event.receiver.ui.EventReceiverUIUtils" %>

<%

    if (!"post".equalsIgnoreCase(request.getMethod())) {
        response.sendError(405);
        return;
    }

    String eventAdapterType = request.getParameter("eventReceiverName");
    String action = request.getParameter("action");

    if (eventAdapterType != null && action != null) {
        EventReceiverAdminServiceStub stub = EventReceiverUIUtils.getEventReceiverAdminService(config, session, request);
        if ("enableStat".equals(action)) {
            stub.setStatisticsEnabled(eventAdapterType, true);
        } else if ("disableStat".equals(action)) {
            stub.setStatisticsEnabled(eventAdapterType, false);
        } else if ("enableTracing".equals(action)) {
            stub.setTracingEnabled(eventAdapterType, true);
        } else if ("disableTracing".equals(action)) {
            stub.setTracingEnabled(eventAdapterType, false);
        }
    }

%>