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
package com.cubrid.cubridmigration.ui.script.handler;

import com.cubrid.cubridmigration.ui.common.UICommonTool;
import com.cubrid.cubridmigration.ui.common.dialog.DetailMessageDialog;
import com.cubrid.cubridmigration.ui.message.Messages;
import com.cubrid.cubridmigration.ui.script.MigrationScript;
import com.cubrid.cubridmigration.ui.wizard.MigrationWizardFactory;
import java.util.List;

/**
 * Action to open the migration script in migration wizard dialog.
 *
 * @author Kevin Cao
 * @version 1.0 - 2012-6-29 created by Kevin Cao
 */
public class OpenMigrationScriptHandler extends BaseMigrationScriptHandler {
    /**
     * Only the selected object is migration script the action will be enabled.
     *
     * @return true if this action supports the selected object
     */
    public boolean isEnabled() {
        return isSingleScriptSelected();
    }

    /**
     * Open migration wizard.
     *
     * @param scripts List<MigrationScript>
     */
    protected void handlerScripts(List<MigrationScript> scripts) {
        try {
            MigrationWizardFactory.openMigrationScript(scripts.get(0));
        } catch (Exception ex) {
            DetailMessageDialog.openError(
                    getShell(), Messages.msgError, Messages.errInvalidScriptFile, ex.getMessage());
            UICommonTool.openErrorBox(getShell(), Messages.errInvalidScriptFile);
        }
    }
}
