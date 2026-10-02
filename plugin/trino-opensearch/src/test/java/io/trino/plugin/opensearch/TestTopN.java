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
import io.trino.plugin.opensearch.TopN.TopNSortItem;
import io.trino.spi.connector.SortOrder;
import org.junit.jupiter.api.Test;
import org.opensearch.search.sort.SortBuilders;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTopN
{
    @Test
    public void testSortBuilderMapsDirectionAndNullOrdering()
    {
        assertThat(new TopNSortItem("field", SortOrder.ASC_NULLS_LAST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.ASC));
        assertThat(new TopNSortItem("field", SortOrder.ASC_NULLS_FIRST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.ASC).missing("_first"));
        assertThat(new TopNSortItem("field", SortOrder.DESC_NULLS_LAST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.DESC));
        assertThat(new TopNSortItem("field", SortOrder.DESC_NULLS_FIRST).toSortBuilder())
                .isEqualTo(SortBuilders.fieldSort("field").order(org.opensearch.search.sort.SortOrder.DESC).missing("_first"));
    }

    @Test
    public void testFromLimitHasNoSortItems()
    {
        TopN topN = TopN.fromLimit(5);

        assertThat(topN.limit()).isEqualTo(5);
        assertThat(topN.sortItems()).isEmpty();
    }

    @Test
    public void testRejectsNegativeLimit()
    {
        assertThatThrownBy(() -> new TopN(-1, ImmutableList.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("limit is negative");
    }
}
