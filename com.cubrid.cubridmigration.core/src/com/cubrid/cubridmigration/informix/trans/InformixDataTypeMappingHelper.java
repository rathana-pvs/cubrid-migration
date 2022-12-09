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

package com.cubrid.cubridmigration.informix.trans;

import java.util.Locale;

import com.cubrid.cubridmigration.core.mapping.AbstractDataTypeMappingHelper;
import com.cubrid.cubridmigration.core.mapping.model.MapItem;
import com.cubrid.cubridmigration.core.mapping.model.MapObject;

/**
 * InformixDataTypeMappingHelper Description
 *
 * @author rathana
 * @version 1.0 
 * @created Aug 22, 2022
 */
public class InformixDataTypeMappingHelper extends AbstractDataTypeMappingHelper{

	public InformixDataTypeMappingHelper() {
		super("INFORMIX2CUBRID",
				"/com/cubrid/cubridmigration/informix/trans/INFORMIX2CUBRID.xml");
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getMapKey(String datatype, String precision, String scale) {
		// TODO Auto-generated method stub
		if(!datatype.isEmpty()) {
			String type = datatype.toLowerCase(Locale.ENGLISH);
			if(type.contains("datetime")) {
				return "datetime";
			}else if(type.contains("interval")){
				return "interval";
			}else if(type.contains("sendreceive")) {
				return "set";
			}
		}
		
		return datatype;
	}
	
	/**
	 * getDataTypeMapping
	 * 
	 * @param datatype String
	 * @param precision Integer
	 * @param scale Integer
	 * @return DataTypeMappingItem
	 */
	public MapObject getTargetFromPreference(String datatype, Integer precision, Integer scale) {
		MapObject obj = super.getTargetFromPreference(datatype, precision, scale);
		
		if(obj == null) {
			MapObject mapObject = new MapObject();
			mapObject.setDatatype("varchar");
			mapObject.setPrecision(precision.toString());
			return mapObject;
		}
		
		return obj;
		
	}

}
