/*
 * Copyright 2015 Francesco Pontillo
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.frapontillo.pulse.crowd.remstopword;

import com.github.frapontillo.pulse.crowd.data.entity.Message;
import com.github.frapontillo.pulse.rx.PulseSubscriber;
import com.github.frapontillo.pulse.spi.IPlugin;
import com.github.frapontillo.pulse.spi.IPluginConfig;
import com.github.frapontillo.pulse.util.PulseLogger;
import com.google.gson.JsonElement;
import org.apache.logging.log4j.Logger;
import rx.Observable;

/**
 * Abstract stop word remover class, handles conversion of the {@link IPluginConfig} from a {@link
 * JsonElement}.
 *
 * @author Francesco Pontillo
 */
public abstract class StopWordRemover<Config extends IPluginConfig<Config>>
        extends IPlugin<Message, Message, Config> {
    private final Logger logger = PulseLogger.getLogger(StopWordRemover.class);

    protected abstract boolean isTokenStopWord(String token, String language,
            Config stopWordConfig);

    protected abstract boolean isTagStopWord(String tag, String language, Config stopWordConfig);

    protected abstract boolean isCategoryStopWord(String category, String language,
            Config stopWordConfig);

    @Override protected Observable.Operator<Message, Message> getOperator(Config parameters) {
        return subscriber -> new PulseSubscriber<Message>(subscriber) {
            @Override public void onNext(Message message) {
                reportElementAsStarted(message.getId());
                try {
                    processMessage(message, parameters);
                } catch (Exception e) {
                    logger.error("Handled exception", e);
                }
                reportElementAsEnded(message.getId());
                subscriber.onNext(message);
            }

            @Override public void onCompleted() {
                reportPluginAsCompleted();
                super.onCompleted();
            }

            @Override public void onError(Throwable e) {
                reportPluginAsErrored();
                super.onError(e);
            }
        };
    }

    protected abstract void processMessage(Message message, Config stopWordConfig);
}
