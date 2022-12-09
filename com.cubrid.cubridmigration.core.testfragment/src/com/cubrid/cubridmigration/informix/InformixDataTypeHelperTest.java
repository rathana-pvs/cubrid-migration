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
package com.cubrid.cubridmigration.informix;

import org.junit.Assert;
import org.junit.Test;

import com.cubrid.cubridmigration.core.dbobject.Column;

/**
 * InformixDataTypeHelperTest Description
 * @author rathana
 * @version 1.0
 * @created Dec 7, 2022
 */
public class InformixDataTypeHelperTest {

	InformixDataTypeHelper helper = InformixDataTypeHelper.getInstance(null);
	
	
	
	private String getDataTypeStr(String colType, Integer precision, Integer scale) {
		Column column = new Column();
		column.setDataType(colType);
		column.setPrecision(precision);
		column.setScale(scale);
		return helper.getShownDataType(column);
	}
	
	/**
	 * testMakeType
	 */
	@Test
	public final void testMakeType() {
		Integer precision = 0;
		Integer scale = 2;

		Assert.assertEquals("varchar(255)",
				getDataTypeStr("varchar", 255, scale));
		Assert.assertEquals("char(255)",
				getDataTypeStr("char", 255, scale));
		Assert.assertEquals("nchar(255)",
				getDataTypeStr("nchar", 255, scale));
		Assert.assertEquals("integer",
				getDataTypeStr("integer", 5, scale));
		Assert.assertEquals("serial",
				getDataTypeStr("serial", 4, scale));
	}
	
}
