/*
 * Copyright (c) 2016 CUBRID Corporation.
 *
 * Redistribution and use in source and binary forms, with or without modification, 
 * are permitted provided that the following conditions are met: 
 *
 * - Redistributions of source code must retain the above copyright notice, 
 *   this list of conditions and the following disclaimer. 
 *
 * - Redistributions in binary form must reproduce the above copyright notice, 
 *   this list of conditions and the following disclaimer in the documentation 
 *   and/or other materials provided with the distribution. 
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors 
 *   may be used to endorse or promote products derived from this software without 
 *   specific prior written permission. 
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND 
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED 
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. 
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, 
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, 
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, 
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, 
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) 
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY 
 * OF SUCH DAMAGE. 
 *
 */
package com.cubrid.cubridmigration.postgresql.trans;

import com.cubrid.cubridmigration.core.datatype.DataTypeConstant;
import com.cubrid.cubridmigration.core.datatype.DataTypeInstance;
import com.cubrid.cubridmigration.core.dbobject.Column;
import com.cubrid.cubridmigration.core.engine.config.MigrationConfiguration;
import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.trans.DBTransformHelper;
import com.cubrid.cubridmigration.cubrid.CUBRIDDataTypeHelper;
import com.cubrid.cubridmigration.cubrid.trans.ToCUBRIDDataConverterFacade;

/**
 * PostgreSQL2CUBRIDTransformHelper Description
 *
 * @author rathana
 * @version 1.0 
 * @created Feb 6, 2023
 */
public class PostgreSQL2CUBRIDTransformHelper extends DBTransformHelper{

	public PostgreSQL2CUBRIDTransformHelper(AbstractDataTypeMappingHelper dataTypeMapping,
			ToCUBRIDDataConverterFacade cf) {
		super(dataTypeMapping, cf);
		// TODO Auto-generated constructor stub
	}

	/**
	 * adjust precision of a column
	 * 
	 * @param srcColumn Column
	 * @param cubColumn Column
	 * @param config MigrationConfiguration
	 */
	protected void adjustPrecision(Column srcColumn, Column cubColumn, MigrationConfiguration config) {
		String dtBasic = cubColumn.getSubDataType() == null ? cubColumn.getDataType()
				: cubColumn.getSubDataType();
		long expectedPrecision = (long) cubColumn.getPrecision();

		CUBRIDDataTypeHelper cubDTHelper = CUBRIDDataTypeHelper.getInstance(null);
		//MSSQLDataTypeHelper dtSrcHelper = MSSQLDataTypeHelper.getInstance(null);
		// 3. get CUBRID precision
		if (cubDTHelper.isString(dtBasic)) {
			long maxValue = DataTypeConstant.CUBRID_MAXSIZE;
			expectedPrecision = Math.min(maxValue, expectedPrecision);
			cubColumn.setPrecision((int) expectedPrecision);
		} else if (cubDTHelper.isStrictNumeric(dtBasic)
				&& expectedPrecision > DataTypeConstant.NUMERIC_MAX_PRECISIE_SIZE) {
			DataTypeInstance dti = new DataTypeInstance();
			dti.setName(DataTypeConstant.CUBRID_VARCHAR);
			dti.setPrecision((int) expectedPrecision + 2);
			cubColumn.setDataTypeInstance(dti);
			cubColumn.setJdbcIDOfDataType(DataTypeConstant.CUBRID_DT_VARBIT);
		} else if (cubDTHelper.isBinary(cubColumn.getDataType())) {
			int maxValue = DataTypeConstant.CUBRID_MAXSIZE;
			expectedPrecision = expectedPrecision * 8;
			expectedPrecision = Math.min(expectedPrecision, maxValue);
			cubColumn.setPrecision((int) expectedPrecision);
		}
	}

}
