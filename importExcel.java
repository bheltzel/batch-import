	public String importSheet(ProcessingElement processingElement, XSSFWorkbook workbook, int tableColumnCount) throws SQLException {
		XSSFSheet sheet = workbook.getSheet(processingElement.getWorksheetName());
		StringBuilder sqlDataPart = new StringBuilder(SQL_QUERY_BUFFER);
		int recourdCount = 0;
		boolean skipColumnMismatchWarning = false;
		String errorMessage = "";
		int lastDataRow = sheet.getLastRowNum();
		int errorCount = 0;
		
		Statement statement = null;
		Connection conn = null;		
		logger.info("Processing sheet " + sheet.getSheetName());
		
		try {
			conn = DatabaseManager.getConnection(dataSourceName);
			statement = conn.createStatement();
		} catch (PropertyVetoException e1) {
			e1.printStackTrace();
		}
		
		for (int rowIdx = firstDataRowNr - 1; rowIdx <= lastDataRow; rowIdx++) {
			XSSFRow row = sheet.getRow(rowIdx);
			
			if (row != null) {
				int maxColIdx = row.getLastCellNum();
				if (!skipColumnMismatchWarning && tableColumnCount != maxColIdx) {
					String errorMsg = "Column count mismatch. Table column #: " + tableColumnCount + 
							". Sheet " + sheet.getSheetName() + " column #: " +  maxColIdx;
					logger.warn(errorMsg);
					errorHandler.addNewError(errorMsg);
					skipColumnMismatchWarning = true;
				}
				
				sqlDataPart.append("INSERT INTO " + processingElement.tableName + " VALUES (");
				
				for (int j = 0; j < tableColumnCount; j++) {
					if(j != 0) {
						sqlDataPart.append(",");
					} 
					if(j <= maxColIdx) {
						sqlDataPart.append(formatCellValue(row.getCell(j)));
					} else {
						sqlDataPart.append("''");
					}
				}
				
				sqlDataPart.append("); ");
				recourdCount++;
				statement.addBatch(sqlDataPart.toString());
				sqlDataPart.setLength(0);
				
				if ((recourdCount) % SQL_COUNT_TRIGGER == 0 || (rowIdx == lastDataRow)) {
					try {
						statement.executeBatch();
					} catch (java.sql.BatchUpdateException e) {
						int[] updateCounts = e.getUpdateCounts();
						for (int i = 0; i < updateCounts.length; i++) {
							if (updateCounts[i] == Statement.EXECUTE_FAILED) {
								logger.error("Failed to execute record at: " + (i + 1));
								errorCount++;
								errorMessage += "Row " + (rowIdx - updateCounts.length + i + 1) + ": " + e.getMessage() + "<br>";
								rowIdx = (rowIdx - updateCounts.length + i + 1);
								recourdCount = 0;
								
								break;
							}
						}
						
					} catch (SQLException se) {
						se.printStackTrace();
					}
				}
			}
			if (errorCount == 5) {
				logger.info("error count hit 5");
				errorMessage += "Stopped Transaction at 5 Errors.";
				break;
			} 
		}
		if(statement != null && !statement.isClosed()) {
			statement.close();
		}
    	DatabaseManager.releaseConnection(conn);
    	
    	if (errorMessage.equals("")) {
			errorMessage = "Success";
		}
		return errorMessage;
	}