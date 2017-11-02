package org.wso2.carbon.event.output.adapter.rdbms.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.Attribute;
import org.wso2.carbon.event.output.adapter.core.exception.ConnectionUnavailableException;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;
import org.wso2.carbon.event.output.adapter.rdbms.RDBMSEventAdapter;

public class RDBMSWriter implements Runnable {
	
	private static final Log log = LogFactory.getLog(RDBMSWriter.class);
	
	private RDBMSEventAdapter adapter;
	private DataSource dataSource;
	private ExecutionInfo executionInfo = null;
	
	public RDBMSWriter(RDBMSEventAdapter adapter) {
		this.adapter = adapter;
	}
	
	public synchronized void run() {
		int messagesInQueue = RDBMSEventAdapter.messageQueue.size();
		
		if (messagesInQueue != 0) {
			// Number of messages written to database during run
			int messagesWritten = 0;
		
			if (dataSource == null)
				dataSource = adapter.getDataSource();
			
			if (dataSource != null) {
				if (executionInfo == null)
					executionInfo = adapter.getExecutionInfo();
			
				if (executionInfo != null) {
					Connection con = null;
					PreparedStatement stmt = null;
				
					if (log.isDebugEnabled()) {
						log.debug("RDBMSWriter running...");
						log.debug("Number of messages in queue: " + messagesInQueue);
					}
				
					try {
						con = dataSource.getConnection();
						con.setAutoCommit(false);
			
						if (executionInfo.isUpdateMode()) 
							stmt = con.prepareStatement(executionInfo.getPreparedUpdateStatement());
						else
							stmt = con.prepareStatement(executionInfo.getPreparedInsertStatement());
					
						int batchSize = executionInfo.getBatchSize();
						int messages = 0, messagesInTrans  = 0;
			
						Map<String, Object> message = null;
					
						while (!RDBMSEventAdapter.messageQueue.isEmpty()) {
							message = RDBMSEventAdapter.messageQueue.poll();
						
							if (message == null)
								break;
						
							messages++;
						
							if (executionInfo.isUpdateMode()) 
								populateStatement(message, stmt, executionInfo.getUpdateQueryColumnOrder());
							else
								populateStatement(message, stmt, executionInfo.getInsertQueryColumnOrder());
				 
							stmt.addBatch();
						
							if (messages % batchSize == 0) {
								// Execute batch
								executeBatch(stmt);
								// Update counters
								messagesWritten += batchSize;
								messagesInTrans += batchSize;
							
								// If we have reached the maximum size of a transaction, commit the transaction
								if (messagesInTrans >= 2000) {
									con.commit();
									// Reset counter
									messagesInTrans = 0;
								}
							
								// Reset number of messages in batch
								messages = 0;
							}
						}
			 
						// If there are any messages left, execute a new batch
						if (messages > 0) {
							executeBatch(stmt);
							messagesWritten += messages;
						}
			 
						// Commit the transaction
						con.commit();
					}
					catch (Exception e) {
						log.error("Error publishing data to RDBMS:" + e.getMessage());
					}
					finally {
						cleanupConnections(stmt, con);
					}
				}
			
				if (log.isDebugEnabled()) {
					log.debug("RDBMSWriter done...");
					log.debug("Number of messages written:" + messagesWritten);
					log.debug("Number of messages in queue: " + RDBMSEventAdapter.messageQueue.size());
				}
			}
		}
	}
	
	private void executeBatch(PreparedStatement stmt) throws OutputEventAdapterException {
		try {
			int[] result = stmt.executeBatch();
			
			for (int i = 0; i < result.length; i++) {
				if (result[i] == PreparedStatement.EXECUTE_FAILED) {
					log.error("Execute batch failed for row:" + i);
				}
			}
		}
		catch (Exception sqlex) {
			throw new OutputEventAdapterException("Execute batch failed: " + 
					sqlex.getMessage(), sqlex);
		}
	}
	
	/**
	 * Populating column values to table Insert query
	 */
	private void populateStatement(Map<String, Object> map, PreparedStatement stmt, List<Attribute> colOrder)
			throws OutputEventAdapterException 
	{
		Attribute attribute = null;

		try {
			for (int i = 0; i < colOrder.size(); i++) {
				attribute = colOrder.get(i);
				Object value = map.get(attribute.getName());
				
				if(value != null && attribute != null && attribute.getType() != null) {
					switch (attribute.getType()) {
					case INT:
						if (value != null)
							stmt.setInt(i + 1, (Integer) value);
						break;
					case LONG:
						if (value != null)
							stmt.setLong(i + 1, (Long) value);
						break;
					case FLOAT:
						if (value != null)
							stmt.setFloat(i + 1, (Float) value);
						break;
					case DOUBLE:
						if (value != null)
							stmt.setDouble(i + 1, (Double) value);
						break;
					case STRING:
						if (value != null)
							stmt.setString(i + 1, (String) value);
						break;
					case BOOL:
						if (value != null)
							stmt.setBoolean(i + 1, (Boolean) value);
						break;
					}
				} else {
					stmt.setNull(i + 1, java.sql.Types.VARCHAR);
				}
				
			}
		} catch (SQLException e) {
			cleanupConnections(stmt, null);
			throw new OutputEventAdapterException("Cannot set value to attribute name " + attribute.getName() + ". "
					+ "Hence dropping the event." + e.getMessage(), e);
		}
	}
	
	private void cleanupConnections(Statement stmt, Connection connection) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.error("unable to close statement." + e.getMessage(), e);
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error("unable to close connection." + e.getMessage(), e);
            }
        }
    }

}
