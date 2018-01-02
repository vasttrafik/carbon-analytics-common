package org.wso2.carbon.event.output.adapter.rdbms.internal;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.wso2.carbon.databridge.commons.Attribute;
import org.wso2.carbon.event.output.adapter.core.exception.OutputEventAdapterException;
import org.wso2.carbon.event.output.adapter.rdbms.RDBMSEventAdapter;

public class RDBMSWriter implements Runnable {
	
	private static final Log log = LogFactory.getLog(RDBMSWriter.class);
	
	private Connection con = null;
	
	private int batchSize = 500;
	HashMap<String, PreparedStatement> statements;
	
	public RDBMSWriter(int batchSize) {
		this.batchSize = batchSize;
	}
	
	public synchronized void run() {

		Map<String, Object> message = null;
		ExecutionInfo executionInfo = null;
		statements = new HashMap<String, PreparedStatement>();

		int messages = 0;

		try {

			PreparedStatement stmt = null;
			con = null;
			
			int queueSize = RDBMSEventAdapter.messageQueue.size();
			
			if (log.isDebugEnabled()) {
				log.debug("Queue now contains: " + queueSize + " messages");
			}
			
			for(int i = 0; i < queueSize; i++) {

				// Take a message from the  queue
				executionInfo = RDBMSEventAdapter.messageQueue.poll();

				if (executionInfo == null)
					break;

				if (con == null) {
					con = RDBMSEventAdapter.dataSource.getConnection();
					con.setAutoCommit(false);
				}

				if (executionInfo.isUpdateMode()) {

					if (statements.containsKey(executionInfo.getPreparedUpdateStatement())) {
						stmt = statements.get(executionInfo.getPreparedUpdateStatement());
					} else {
						PreparedStatement stmnt = con.prepareStatement(executionInfo.getPreparedUpdateStatement());
						statements.put(executionInfo.getPreparedUpdateStatement(), stmnt);
						stmt = stmnt;
					}

				} else {

					if (statements.containsKey(executionInfo.getPreparedInsertStatement())) {
						stmt = statements.get(executionInfo.getPreparedInsertStatement());
					} else {
						PreparedStatement stmnt = con.prepareStatement(executionInfo.getPreparedInsertStatement());
						statements.put(executionInfo.getPreparedInsertStatement(), stmnt);
						stmt = stmnt;
					}

				}

				message = executionInfo.getMessage();
				messages++;

				if (executionInfo.isUpdateMode())
					populateStatement(message, stmt, executionInfo.getUpdateQueryColumnOrder());
				else
					populateStatement(message, stmt, executionInfo.getInsertQueryColumnOrder());

				stmt.addBatch();

				if (messages % batchSize == 0 || RDBMSEventAdapter.messageQueue.size() == 0) {
					// Execute batch

					for (PreparedStatement stmnt : statements.values()) {
						executeBatch(stmnt);
					}
					con.commit();

					if (log.isDebugEnabled()) {
						log.debug("Commited batch. Wrote " + messages + " statements to database");
					}

					// Reset number of messages
					messages = 0;
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
			log.error("Error publishing data to RDBMS:" + e.getMessage());
			cleanupConnections();
		} finally {
			cleanupConnections();
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
			cleanupConnections();
			throw new OutputEventAdapterException("Cannot set value to attribute name " + attribute.getName() + ". "
					+ "Hence dropping the event." + e.getMessage(), e);
		}
	}
	
	private void cleanupConnections() {
		
		if(statements != null) {
			for(PreparedStatement stmnt : statements.values()) {
				try {
					stmnt.close();
				} catch (SQLException e) {
					log.error("Unable to close statement." + e.getMessage(), e);
				}
			}
		}

        if (con != null) {
            try {
                con.close();
            } catch (SQLException e) {
                log.error("Unable to close connection." + e.getMessage(), e);
            }
        }
    }

}
