package com.zavakid.dbcompare.common;

import java.beans.PropertyEditorSupport;

import com.zavakid.dbcompare.work.WorkMode;

/**
 * @author zebin.xuzb 2012-12-21 下午4:34:30
 * @since 4.1.3
 */
public class WorkModeEditor extends PropertyEditorSupport {

    @Override
    public void setAsText(String text) throws IllegalArgumentException {
        setValue(WorkMode.valueOf(text));
    }

}
