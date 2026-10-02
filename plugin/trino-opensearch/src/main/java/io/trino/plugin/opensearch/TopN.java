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
import io.trino.spi.connector.SortOrder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public record TopN(long limit, List<TopNSortItem> sortItems)
{
    public TopN
    {
        checkArgument(limit >= 0, "limit is negative: %s", limit);
        sortItems = ImmutableList.copyOf(requireNonNull(sortItems, "sortItems is null"));
    }

    public static TopN fromLimit(long limit)
    {
        return new TopN(limit, ImmutableList.of());
    }

    /**
     * @param unmappedType the OpenSearch field type used when an index of an alias or wildcard table does not map the field,
     *         without it the sort fails instead of treating the field as missing
     */
    public record TopNSortItem(String field, SortOrder order, Optional<String> unmappedType)
    {
        // sorting by _doc (index order) gets special treatment in OpenSearch and is more efficient
        public static final TopNSortItem SORT_BY_DOC = new TopNSortItem("_doc", SortOrder.ASC_NULLS_LAST, Optional.empty());

        public TopNSortItem
        {
            requireNonNull(field, "field is null");
            requireNonNull(order, "order is null");
            requireNonNull(unmappedType, "unmappedType is null");
        }

        public SortBuilder<?> toSortBuilder()
        {
            FieldSortBuilder sortBuilder = SortBuilders.fieldSort(field);
            unmappedType.ifPresent(sortBuilder::unmappedType);
            if (order.isAscending()) {
                sortBuilder.order(org.opensearch.search.sort.SortOrder.ASC);
            }
            else {
                sortBuilder.order(org.opensearch.search.sort.SortOrder.DESC);
            }
            if (order.isNullsFirst()) {
                // the default for missing values is _last in both directions
                sortBuilder.missing("_first");
            }
            return sortBuilder;
        }
    }
}
