/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.opensearch;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.trino.plugin.base.session.SessionPropertiesProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.session.PropertyMetadata;

import java.util.List;

import static io.trino.spi.session.PropertyMetadata.booleanProperty;

public final class OpenSearchSessionProperties
        implements SessionPropertiesProvider
{
    private static final String PROJECTION_PUSHDOWN_ENABLED = "projection_pushdown_enabled";
    private static final String AGGREGATION_PUSHDOWN_ENABLED = "aggregation_pushdown_enabled";
    private static final String TEXT_EQUALITY_PUSHDOWN_ENABLED = "text_equality_pushdown_enabled";

    private final List<PropertyMetadata<?>> sessionProperties;

    @Inject
    public OpenSearchSessionProperties(OpenSearchConfig openSearchConfig)
    {
        sessionProperties = ImmutableList.<PropertyMetadata<?>>builder()
                .add(booleanProperty(
                        PROJECTION_PUSHDOWN_ENABLED,
                        "Read only required fields from a row type",
                        openSearchConfig.isProjectionPushdownEnabled(),
                        false))
                .add(booleanProperty(
                        AGGREGATION_PUSHDOWN_ENABLED,
                        "Push down supported aggregations to OpenSearch",
                        openSearchConfig.isAggregationPushdownEnabled(),
                        false))
                .add(booleanProperty(
                        TEXT_EQUALITY_PUSHDOWN_ENABLED,
                        "Push down equality predicates on text fields to their keyword sub-field, assuming all documents were indexed under the current sub-field mapping",
                        openSearchConfig.isTextEqualityPushdownEnabled(),
                        false))
                .build();
    }

    @Override
    public List<PropertyMetadata<?>> getSessionProperties()
    {
        return sessionProperties;
    }

    public static boolean isProjectionPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(PROJECTION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isAggregationPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(AGGREGATION_PUSHDOWN_ENABLED, Boolean.class);
    }

    public static boolean isTextEqualityPushdownEnabled(ConnectorSession session)
    {
        return session.getProperty(TEXT_EQUALITY_PUSHDOWN_ENABLED, Boolean.class);
    }
}
