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
package io.trino.plugin.opensearch.sql;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSqlIdentifiers
{
    @Test
    public void testQuote()
    {
        assertThat(SqlIdentifiers.isQuotable("metric_logs_20260701")).isTrue();
        assertThat(SqlIdentifiers.quote("metric_logs_20260701")).isEqualTo("`metric_logs_20260701`");
        assertThat(SqlIdentifiers.quote("tenant-id")).isEqualTo("`tenant-id`");
        assertThat(SqlIdentifiers.quote("naïve")).isEqualTo("`naïve`");
    }

    @Test
    public void testRejected()
    {
        for (String name : new String[] {"", "a`b", "a\nb", "a\u0000b", "a\u007fb"}) {
            assertThat(SqlIdentifiers.isQuotable(name)).as(name).isFalse();
            assertThatThrownBy(() -> SqlIdentifiers.quote(name)).isInstanceOf(IllegalArgumentException.class);
        }
    }
}
