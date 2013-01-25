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
package com.zavakid.dbcompare.config.impl;

import com.zavakid.dbcompare.config.Configuration;

/**
 * @author Zavakid 2013-1-25 下午6:23:27
 * @since 0.0.1
 */
public class StaticConfiguration implements Configuration {

    private final int maxBatchSize;
    private final int maxWorkers;

    public static Configuration create(int maxBatchSize, int maxWorkers) {
        return new StaticConfiguration(maxBatchSize, maxWorkers);
    }

    public StaticConfiguration(int maxBatchSize, int maxWorkers){
        this.maxBatchSize = maxBatchSize;
        this.maxWorkers = maxWorkers;
    }

    @Override
    public int getMaxBatchSize() {
        return maxBatchSize;
    }

    @Override
    public int getMaxWorkers() {
        return maxWorkers;
    }

}
