/*
   Copyright 2013 Zava (http://www.zavakid.com)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package com.zavakid.dbcompare.strategy;

import java.util.concurrent.ExecutorService;

import com.zavakid.dbcompare.config.Configuration;
import com.zavakid.dbcompare.table.TablePair;

/**
 * CompareStrategy will compare table pair with it's own strategy
 * 
 * @author Zavakid 2013-1-25 下午5:59:20
 * @since 0.0.1
 */
public interface CompareStrategy {

    /**
     * @param tablePair
     * @param worker
     */
    void compare(TablePair tablePair, ExecutorService workers, Configuration configuration) throws Exception;
}
