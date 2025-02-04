/*
 * Copyright (C) 2008 Search Solution Corporation.
 * Copyright (C) 2016 CUBRID Corporation.
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
package com.cubrid.cubridmigration.mssql.dbobj;

import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

public class MSSQLPartitionSchemasTest {

    @Test
    public void testMSSQLPartitionSchemas() {
        MSSQLPartitionSchemas ps = new MSSQLPartitionSchemas();
        ps.setBoundaryValueOnRight(true);
        Assert.assertTrue(ps.getBoundaryValueOnRight());
        ps.setDataSpaceId(0L);
        Assert.assertEquals(new Long(0L), ps.getDataSpaceId());
        ps.setFunctionId(1);
        Assert.assertEquals(1, ps.getFunctionId());
        ps.setName("name");
        ps.setParameterId(2);
        Assert.assertEquals(2, ps.getParameterId());
        ps.setPartitionCount(3);
        Assert.assertEquals(3, ps.getPartitionCount());
        ArrayList<String> partitionRangeValues = new ArrayList<String>();
        ps.setPartitionRangeValues(partitionRangeValues);
        Assert.assertEquals(partitionRangeValues, ps.getPartitionRangeValues());
        ps.setSystemType("st");
        Assert.assertEquals("st", ps.getSystemType());
    }
}
