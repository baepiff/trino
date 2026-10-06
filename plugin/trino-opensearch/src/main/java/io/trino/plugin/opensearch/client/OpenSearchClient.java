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
package io.trino.plugin.opensearch.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.NullNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonMapperProvider;
import io.airlift.log.Logger;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import io.trino.plugin.opensearch.AwsSecurityConfig;
import io.trino.plugin.opensearch.OpenSearchConfig;
import io.trino.plugin.opensearch.PasswordConfig;
import io.trino.plugin.opensearch.TopN.TopNSortItem;
import io.trino.spi.TrinoException;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.reactor.IOReactorConfig;
import org.apache.hc.core5.util.Timeout;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;

import javax.net.ssl.SSLContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.trino.plugin.base.ssl.SslUtils.createSSLContext;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_CONNECTION_ERROR;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_INVALID_METADATA;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_INVALID_RESPONSE;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_QUERY_FAILURE;
import static io.trino.plugin.opensearch.OpenSearchErrorCode.OPENSEARCH_SSL_INITIALIZATION_FAILURE;
import static java.lang.StrictMath.toIntExact;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.hc.core5.http.ContentType.APPLICATION_JSON;
import static org.opensearch.action.search.SearchType.QUERY_THEN_FETCH;

public class OpenSearchClient
{
    private static final Logger LOG = Logger.get(OpenSearchClient.class);

    private static final JsonCodec<SearchShardsResponse> SEARCH_SHARDS_RESPONSE_CODEC = jsonCodec(SearchShardsResponse.class);
    private static final JsonCodec<NodesResponse> NODES_RESPONSE_CODEC = jsonCodec(NodesResponse.class);
    private static final JsonCodec<CountResponse> COUNT_RESPONSE_CODEC = jsonCodec(CountResponse.class);
    private static final JsonMapper JSON_MAPPER = new JsonMapperProvider().get();

    private static final Pattern ADDRESS_PATTERN = Pattern.compile("((?<cname>[^/]+)/)?(?<ip>.+):(?<port>\\d+)");
    private static final Set<String> NODE_ROLES = ImmutableSet.of("data", "data_content", "data_hot", "data_warm", "data_cold", "data_frozen");

    private final BackpressureRestHighLevelClient client;
    private final int scrollSize;
    private final Duration scrollTimeout;

    private final AtomicReference<Set<OpenSearchNode>> nodes = new AtomicReference<>(ImmutableSet.of());
    private final ScheduledExecutorService executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("NodeRefresher"));
    private final AtomicBoolean started = new AtomicBoolean();
    private final Duration refreshInterval;
    private final boolean tlsEnabled;
    private final boolean ignorePublishAddress;

    private final TimeStat searchStats = new TimeStat(MILLISECONDS);
    private final TimeStat nextPageStats = new TimeStat(MILLISECONDS);
    private final TimeStat countStats = new TimeStat(MILLISECONDS);
    private final TimeStat backpressureStats = new TimeStat(MILLISECONDS);

    @Inject
    public OpenSearchClient(
            OpenSearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig)
    {
        client = createClient(config, awsSecurityConfig, passwordConfig, backpressureStats);

        this.ignorePublishAddress = config.isIgnorePublishAddress();
        this.scrollSize = config.getScrollSize();
        this.scrollTimeout = config.getScrollTimeout();
        this.refreshInterval = config.getNodeRefreshInterval();
        this.tlsEnabled = config.isTlsEnabled();
    }

    @PostConstruct
    public void initialize()
    {
        if (!started.getAndSet(true)) {
            // do the first refresh eagerly
            refreshNodes();

            executor.scheduleWithFixedDelay(this::refreshNodes, refreshInterval.toMillis(), refreshInterval.toMillis(), MILLISECONDS);
        }
    }

    @PreDestroy
    public void close()
            throws IOException
    {
        executor.shutdownNow();
        client.close();
    }

    private void refreshNodes()
    {
        // discover other nodes in the cluster and add them to the client
        try {
            Set<OpenSearchNode> nodes = fetchNodes();

            HttpHost[] hosts = nodes.stream()
                    .map(OpenSearchNode::address)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(address -> {
                        try {
                            return HttpHost.create(format("%s://%s", tlsEnabled ? "https" : "http", address));
                        }
                        catch (URISyntaxException e) {
                            throw new RuntimeException(e);
                        }
                    })
                    .toArray(HttpHost[]::new);

            if (hosts.length > 0 && !ignorePublishAddress) {
                client.getLowLevelClient().setHosts(hosts);
            }

            this.nodes.set(nodes);
        }
        catch (Throwable e) {
            // Catch all exceptions here since throwing an exception from executor#scheduleWithFixedDelay method
            // suppresses all future scheduled invocations
            LOG.error(e, "Error refreshing nodes");
        }
    }

    @SuppressWarnings("deprecation")
    private static BackpressureRestHighLevelClient createClient(
            OpenSearchConfig config,
            Optional<AwsSecurityConfig> awsSecurityConfig,
            Optional<PasswordConfig> passwordConfig,
            TimeStat backpressureStats)
    {
        RestClientBuilder builder = RestClient.builder(
                config.getHosts().stream()
                        .map(httpHost -> new HttpHost(config.isTlsEnabled() ? "https" : "http", httpHost, config.getPort()))
                        .toArray(HttpHost[]::new));

        builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder
                .setConnectTimeout(Timeout.ofMilliseconds(config.getConnectTimeout().toMillis()))
                .setResponseTimeout(Timeout.ofMilliseconds(config.getRequestTimeout().toMillis())));

        builder.setHttpClientConfigCallback(clientBuilder -> {
            IOReactorConfig reactorConfig = IOReactorConfig.custom()
                    .setIoThreadCount(config.getHttpThreadCount())
                    .build();

            clientBuilder.setIOReactorConfig(reactorConfig);

            if (config.isTlsEnabled()) {
                Optional<SSLContext> sslContext = buildSslContext(config.getKeystorePath(), config.getKeystorePassword(), config.getTrustStorePath(), config.getTruststorePassword());
                ClientTlsStrategyBuilder tlsStrategyBuilder = ClientTlsStrategyBuilder.create();
                sslContext.ifPresent(tlsStrategyBuilder::setSslContext);
                if (!config.isVerifyHostnames()) {
                    tlsStrategyBuilder.setHostnameVerifier(NoopHostnameVerifier.INSTANCE);
                }
                clientBuilder.setConnectionManager(PoolingAsyncClientConnectionManagerBuilder.create()
                        .setMaxConnPerRoute(config.getMaxHttpConnections())
                        .setMaxConnTotal(config.getMaxHttpConnections())
                        .setTlsStrategy(tlsStrategyBuilder.buildAsync())
                        .build());
            }
            else {
                clientBuilder.setConnectionManager(PoolingAsyncClientConnectionManagerBuilder.create()
                        .setMaxConnPerRoute(config.getMaxHttpConnections())
                        .setMaxConnTotal(config.getMaxHttpConnections())
                        .build());
            }

            passwordConfig.ifPresent(securityConfig -> {
                BasicCredentialsProvider credentials = new BasicCredentialsProvider();
                credentials.setCredentials(new AuthScope(null, -1), new UsernamePasswordCredentials(securityConfig.getUser(), securityConfig.getPassword().toCharArray()));
                clientBuilder.setDefaultCredentialsProvider(credentials);
            });

            awsSecurityConfig.ifPresent(securityConfig -> clientBuilder.addExecInterceptorLast("AwsRequestSigner", new AwsRequestSigner(
                    securityConfig.getRegion(),
                    securityConfig.getDeploymentType(),
                    getAwsCredentialsProvider(securityConfig))));

            return clientBuilder;
        });

        return new BackpressureRestHighLevelClient(builder, config, backpressureStats);
    }

    private static AwsCredentialsProvider getAwsCredentialsProvider(AwsSecurityConfig config)
    {
        AwsCredentialsProvider credentialsProvider = DefaultCredentialsProvider.builder().build();

        if (config.getAccessKey().isPresent() && config.getSecretKey().isPresent()) {
            credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
                    config.getAccessKey().get(),
                    config.getSecretKey().get()));
        }

        if (config.getIamRole().isPresent()) {
            StsAssumeRoleCredentialsProvider.Builder credentialsProviderBuilder = StsAssumeRoleCredentialsProvider.builder()
                    .stsClient(StsClient.builder()
                            .region(Region.of(config.getRegion()))
                            .credentialsProvider(credentialsProvider)
                            .build())
                    .refreshRequest(request -> {
                        request
                                .roleArn(config.getIamRole().get())
                                .roleSessionName("trino-session");
                        config.getExternalId().ifPresent(request::externalId);
                    });
            credentialsProvider = credentialsProviderBuilder.build();
        }

        return credentialsProvider;
    }

    private static Optional<SSLContext> buildSslContext(
            Optional<File> keyStorePath,
            Optional<String> keyStorePassword,
            Optional<File> trustStorePath,
            Optional<String> trustStorePassword)
    {
        if (keyStorePath.isEmpty() && trustStorePath.isEmpty()) {
            return Optional.empty();
        }

        try {
            return Optional.of(createSSLContext(keyStorePath, keyStorePassword, trustStorePath, trustStorePassword));
        }
        catch (GeneralSecurityException | IOException e) {
            throw new TrinoException(OPENSEARCH_SSL_INITIALIZATION_FAILURE, e);
        }
    }

    private Set<OpenSearchNode> fetchNodes()
    {
        NodesResponse nodesResponse = doRequest("/_nodes/http", NODES_RESPONSE_CODEC::fromJson);

        ImmutableSet.Builder<OpenSearchNode> result = ImmutableSet.builder();
        for (Entry<String, NodesResponse.Node> entry : nodesResponse.getNodes().entrySet()) {
            String nodeId = entry.getKey();
            NodesResponse.Node node = entry.getValue();

            if (!Sets.intersection(node.getRoles(), NODE_ROLES).isEmpty()) {
                Optional<String> address = node.getAddress()
                        .flatMap(OpenSearchClient::extractAddress);

                result.add(new OpenSearchNode(nodeId, address));
            }
        }

        return result.build();
    }

    public Set<OpenSearchNode> getNodes()
    {
        return nodes.get();
    }

    public List<Shard> getSearchShards(String index)
    {
        Map<String, OpenSearchNode> nodeById = getNodes().stream()
                .collect(toImmutableMap(OpenSearchNode::id, Function.identity()));

        SearchShardsResponse shardsResponse = doRequest(format("/%s/_search_shards", index), SEARCH_SHARDS_RESPONSE_CODEC::fromJson);

        ImmutableList.Builder<Shard> shards = ImmutableList.builder();
        List<OpenSearchNode> nodes = ImmutableList.copyOf(nodeById.values());

        for (List<SearchShardsResponse.Shard> shardGroup : shardsResponse.getShardGroups()) {
            Optional<SearchShardsResponse.Shard> candidate = shardGroup.stream()
                    .filter(shard -> shard.getNode() != null && nodeById.containsKey(shard.getNode()))
                    .min(this::shardPreference);

            SearchShardsResponse.Shard chosen;
            OpenSearchNode node;
            if (candidate.isEmpty()) {
                // pick an arbitrary shard with and assign to an arbitrary node
                chosen = shardGroup.stream()
                        .min(this::shardPreference)
                        .get();
                node = nodes.get(chosen.getShard() % nodes.size());
            }
            else {
                chosen = candidate.get();
                node = nodeById.get(chosen.getNode());
            }

            shards.add(new Shard(chosen.getIndex(), chosen.getShard(), node.address()));
        }

        return shards.build();
    }

    private int shardPreference(SearchShardsResponse.Shard left, SearchShardsResponse.Shard right)
    {
        // Favor non-primary shards
        if (left.isPrimary() == right.isPrimary()) {
            return 0;
        }

        return left.isPrimary() ? 1 : -1;
    }

    public boolean indexExists(String index)
    {
        String path = format("/%s/_mappings", index);

        try {
            Response response = client.getLowLevelClient()
                    .performRequest("GET", path);

            return response.getStatusLine().getStatusCode() == 200;
        }
        catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                return false;
            }
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
    }

    public List<String> getIndexes()
    {
        return doRequest("/_cat/indices?h=index,docs.count,docs.deleted&format=json&s=index:asc", body -> {
            try {
                ImmutableList.Builder<String> result = ImmutableList.builder();
                JsonNode root = JSON_MAPPER.readTree(body);
                for (int i = 0; i < root.size(); i++) {
                    String index = root.get(i).get("index").asText();
                    // make sure the index has mappings we can use to derive the schema
                    int docsCount = root.get(i).get("docs.count").asInt();
                    int deletedDocsCount = root.get(i).get("docs.deleted").asInt();
                    if (docsCount == 0 && deletedDocsCount == 0) {
                        try {
                            // without documents, the index won't have any dynamic mappings, but maybe there are some explicit ones
                            if (getIndexMetadata(index).schema().fields().isEmpty()) {
                                continue;
                            }
                        }
                        catch (TrinoException e) {
                            if (e.getErrorCode().equals(OPENSEARCH_INVALID_METADATA.toErrorCode())) {
                                continue;
                            }
                            if (e.getCause() instanceof ResponseException cause && cause.getResponse().getStatusLine().getStatusCode() == 404) {
                                continue;
                            }
                            throw e;
                        }
                    }
                    result.add(index);
                }
                return result.build();
            }
            catch (IOException e) {
                throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    public Map<String, List<String>> getAliases()
    {
        return doRequest("/_aliases", body -> {
            try {
                ImmutableMap.Builder<String, List<String>> result = ImmutableMap.builder();
                JsonNode root = JSON_MAPPER.readTree(body);

                for (Entry<String, JsonNode> element : root.properties()) {
                    JsonNode aliases = element.getValue().get("aliases");
                    Iterator<String> aliasNames = aliases.fieldNames();
                    if (aliasNames.hasNext()) {
                        result.put(element.getKey(), ImmutableList.copyOf(aliasNames));
                    }
                }
                return result.buildOrThrow();
            }
            catch (IOException e) {
                throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    public IndexMetadata getIndexMetadata(String index)
    {
        String path = format("/%s/_mappings", index);

        return doRequest(path, body -> {
            try {
                return parseIndexMetadata(JSON_MAPPER.readTree(body));
            }
            catch (IOException e) {
                throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
            }
        });
    }

    @VisibleForTesting
    static IndexMetadata parseIndexMetadata(JsonNode response)
    {
        // an alias or a wildcard expression returns the mappings of every index behind it
        List<Optional<JsonNode>> typeMappings = ImmutableList.copyOf(response.elements()).stream()
                .map(element -> typeMapping(element.get("mappings")))
                .collect(toImmutableList());

        if (typeMappings.getFirst().isEmpty()) {
            return new IndexMetadata(new IndexMetadata.ObjectType(ImmutableList.of()));
        }
        JsonNode mappings = typeMappings.getFirst().get();

        JsonNode metaNode = nullSafeNode(mappings, "_meta");

        JsonNode metaProperties = nullSafeNode(metaNode, "trino");

        // stay backwards compatible with _meta.presto namespace for meta properties for some releases
        if (metaProperties.isNull()) {
            metaProperties = nullSafeNode(metaNode, "presto");
        }

        IndexMetadata.ObjectType schema = parseType(mappings.get("properties"), metaProperties, true);

        List<JsonNode> allProperties = typeMappings.stream()
                .map(typeMapping -> typeMapping.map(node -> node.get("properties")).orElse(NullNode.getInstance()))
                .collect(toImmutableList());
        return new IndexMetadata(retainConsistentSubFields(schema, allProperties));
    }

    private static Optional<JsonNode> typeMapping(JsonNode mappings)
    {
        if (mappings == null || !mappings.elements().hasNext()) {
            return Optional.empty();
        }
        if (!mappings.has("properties")) {
            // Older versions of OpenSearch supported multiple "type" mappings
            // for a given index. Newer versions support only one and don't
            // expose it in the document. Here we skip it if it's present.
            mappings = mappings.elements().next();

            if (!mappings.has("properties")) {
                return Optional.empty();
            }
        }
        return Optional.of(mappings);
    }

    private static IndexMetadata.ObjectType parseType(JsonNode properties, JsonNode metaProperties, boolean topLevel)
    {
        ImmutableList.Builder<IndexMetadata.Field> result = ImmutableList.builder();
        for (Entry<String, JsonNode> field : properties.properties()) {
            String name = field.getKey();
            JsonNode value = field.getValue();

            // default type is object
            String type = "object";
            if (value.has("type")) {
                type = value.get("type").asText();
            }
            JsonNode metaNode = nullSafeNode(metaProperties, name);
            boolean isArray = !metaNode.isNull() && metaNode.has("isArray") && metaNode.get("isArray").asBoolean();
            boolean asRawJson = !metaNode.isNull() && metaNode.has("asRawJson") && metaNode.get("asRawJson").asBoolean();

            // While it is possible to handle isArray and asRawJson in the same column by creating a ARRAY(VARCHAR) type, we chose not to take
            // this route, as it will likely lead to confusion in dealing with array syntax in Trino and potentially nested array and other
            // syntax when parsing the raw json.
            if (isArray && asRawJson) {
                throw new TrinoException(
                        OPENSEARCH_INVALID_METADATA,
                        format("A column, (%s) cannot be declared as a Trino array and also be rendered as json.", name));
            }

            switch (type) {
                case "date" -> {
                    List<String> formats = ImmutableList.of();
                    if (value.has("format")) {
                        formats = Arrays.asList(value.get("format").asText().split("\\|\\|"));
                    }
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.DateTimeType(formats)));
                }
                case "scaled_float" -> result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.ScaledFloatType(value.get("scaling_factor").asDouble())));
                case "nested", "object" -> {
                    if (value.has("properties")) {
                        result.add(new IndexMetadata.Field(asRawJson, isArray, name, parseType(value.get("properties"), metaNode, false)));
                    }
                    else {
                        LOG.debug("Ignoring empty object field: %s", name);
                    }
                }
                default -> {
                    // sub-fields are only retained for top-level fields, the only ones that use them
                    List<IndexMetadata.SubField> subFields = topLevel ? parseSubFields(value) : ImmutableList.of();
                    result.add(new IndexMetadata.Field(asRawJson, isArray, name, new IndexMetadata.PrimitiveType(type), subFields));
                }
            }
        }

        return new IndexMetadata.ObjectType(result.build());
    }

    private static List<IndexMetadata.SubField> parseSubFields(JsonNode field)
    {
        JsonNode fields = field.get("fields");
        if (fields == null || !fields.isObject()) {
            return ImmutableList.of();
        }
        ImmutableList.Builder<IndexMetadata.SubField> result = ImmutableList.builder();
        for (Entry<String, JsonNode> entry : fields.properties()) {
            JsonNode value = entry.getValue();
            JsonNode type = value.get("type");
            JsonNode ignoreAbove = value.get("ignore_above");
            if (type == null || (ignoreAbove != null && !ignoreAbove.canConvertToInt())) {
                // the sub-field cannot be reasoned about
                continue;
            }
            OptionalInt ignoreAboveValue = OptionalInt.empty();
            if (ignoreAbove != null) {
                ignoreAboveValue = OptionalInt.of(ignoreAbove.asInt());
            }
            Optional<String> normalizer = Optional.ofNullable(value.get("normalizer"))
                    .filter(node -> !node.isNull())
                    .map(JsonNode::asText);
            JsonNode indexed = value.get("index");
            result.add(new IndexMetadata.SubField(
                    entry.getKey(),
                    type.asText(),
                    ignoreAboveValue,
                    normalizer,
                    indexed == null || indexed.asBoolean(true),
                    value.has("null_value")));
        }
        return result.build();
    }

    /**
     * Keeps a sub-field of a top-level field only when every index behind the table declares the field and the
     * sub-field identically, and when no field is copied into the field or the sub-field with {@code copy_to}. A copied
     * value is indexed under the field without being part of its {@code _source}, so a query on the sub-field would
     * match documents whose Trino value differs.
     */
    private static IndexMetadata.ObjectType retainConsistentSubFields(IndexMetadata.ObjectType schema, List<JsonNode> allProperties)
    {
        if (schema.fields().stream().allMatch(field -> field.subFields().isEmpty())) {
            return schema;
        }

        ImmutableSet.Builder<String> copyToTargetsBuilder = ImmutableSet.builder();
        allProperties.forEach(properties -> collectCopyToTargets(properties, copyToTargetsBuilder));
        Set<String> copyToTargets = copyToTargetsBuilder.build();

        ImmutableList.Builder<IndexMetadata.Field> fields = ImmutableList.builder();
        for (IndexMetadata.Field field : schema.fields()) {
            List<IndexMetadata.SubField> subFields = field.subFields().stream()
                    .filter(subField -> !copyToTargets.contains(field.name()) && !copyToTargets.contains(field.name() + "." + subField.name()))
                    .filter(subField -> allProperties.stream().allMatch(properties -> declaresSubField(properties, field, subField)))
                    .collect(toImmutableList());
            fields.add(new IndexMetadata.Field(field.asRawJson(), field.isArray(), field.name(), field.type(), subFields));
        }
        return new IndexMetadata.ObjectType(fields.build());
    }

    private static boolean declaresSubField(JsonNode properties, IndexMetadata.Field field, IndexMetadata.SubField subField)
    {
        JsonNode other = properties.get(field.name());
        return other != null
                && field.type() instanceof IndexMetadata.PrimitiveType primitiveType
                && other.has("type")
                && other.get("type").asText().equals(primitiveType.name())
                && parseSubFields(other).contains(subField);
    }

    private static void collectCopyToTargets(JsonNode node, ImmutableSet.Builder<String> targets)
    {
        if (node == null || !node.isObject()) {
            return;
        }
        JsonNode copyTo = node.get("copy_to");
        if (copyTo != null && copyTo.isTextual()) {
            targets.add(copyTo.asText());
        }
        if (copyTo != null && copyTo.isArray()) {
            copyTo.forEach(target -> targets.add(target.asText()));
        }
        node.forEach(child -> collectCopyToTargets(child, targets));
    }

    private static JsonNode nullSafeNode(JsonNode jsonNode, String name)
    {
        if (jsonNode == null || jsonNode.isNull() || jsonNode.get(name) == null) {
            return NullNode.getInstance();
        }
        return jsonNode.get(name);
    }

    public String executeQuery(String index, String query)
    {
        String path = format("/%s/_search", index);

        Response response;
        try {
            response = client.getLowLevelClient()
                    .performRequest(
                            "GET",
                            path,
                            ImmutableMap.of(),
                            new ByteArrayEntity(query.getBytes(UTF_8), APPLICATION_JSON),
                            new BasicHeader("Accept-Encoding", "application/json"));
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }

        String body;
        try {
            body = EntityUtils.toString(response.getEntity());
        }
        catch (Exception e) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
        }

        return body;
    }

    public String executeSql(String requestBody)
    {
        Response response;
        try {
            response = client.getLowLevelClient()
                    .performRequest(
                            "POST",
                            "/_plugins/_sql",
                            ImmutableMap.of(),
                            new ByteArrayEntity(requestBody.getBytes(UTF_8), APPLICATION_JSON),
                            new BasicHeader("Accept-Encoding", "application/json"));
        }
        catch (ResponseException e) {
            String body;
            try {
                body = EntityUtils.toString(e.getResponse().getEntity());
            }
            catch (Exception ignored) {
                body = "";
            }
            throw new TrinoException(OPENSEARCH_QUERY_FAILURE, "OpenSearch SQL request failed: " + formatSqlError(e.getResponse().getStatusLine().getStatusCode(), body), e);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }

        try {
            return EntityUtils.toString(response.getEntity());
        }
        catch (Exception e) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
        }
    }

    static String formatSqlError(int statusCode, String body)
    {
        String prefix = "HTTP " + statusCode;
        try {
            JsonNode error = JSON_MAPPER.readTree(body).path("error");
            String reason = error.path("reason").asText("");
            if (reason.isEmpty()) {
                return prefix;
            }
            String details = error.path("details").asText("");
            if (details.isEmpty()) {
                return prefix + ": " + reason;
            }
            return prefix + ": " + reason + ": " + details;
        }
        catch (IOException | RuntimeException e) {
            return prefix;
        }
    }

    public SearchResponse beginSearch(String index, int shard, QueryBuilder query, Optional<List<String>> fields, List<String> documentFields, List<TopNSortItem> sortItems, OptionalLong limit)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query);

        if (limit.isPresent() && limit.orElseThrow() < scrollSize) {
            // Safe to cast it to int because scrollSize is int.
            sourceBuilder.size(toIntExact(limit.orElseThrow()));
        }
        else {
            sourceBuilder.size(scrollSize);
        }

        sortItems.forEach(sortItem -> sourceBuilder.sort(sortItem.toSortBuilder()));

        fields.ifPresent(values -> {
            if (values.isEmpty()) {
                sourceBuilder.fetchSource(false);
            }
            else {
                sourceBuilder.fetchSource(values.toArray(new String[0]), null);
            }
        });
        documentFields.forEach(sourceBuilder::docValueField);

        LOG.debug("Begin search: %s:%s, query: %s", index, shard, sourceBuilder);

        SearchRequest request = new SearchRequest(index)
                .searchType(QUERY_THEN_FETCH)
                .preference("_shards:" + shard)
                .scroll(new TimeValue(scrollTimeout.toMillis()))
                .source(sourceBuilder);

        return search(request);
    }

    public SearchResponse beginAggregationSearch(String index, QueryBuilder query, List<AggregationBuilder> aggregations)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query)
                .size(0)
                // accurate total hits are required for count(*), the default stops counting at 10000
                .trackTotalHits(true);
        aggregations.forEach(sourceBuilder::aggregation);

        LOG.debug("Begin aggregation search: %s, query: %s", index, sourceBuilder);

        SearchRequest request = new SearchRequest(index)
                .searchType(QUERY_THEN_FETCH)
                // a failed shard must fail the query, otherwise counts and sums silently miss that shard's documents
                .allowPartialSearchResults(false)
                .source(sourceBuilder);

        return search(request);
    }

    private SearchResponse search(SearchRequest request)
    {
        long start = System.nanoTime();
        try {
            return client.search(request);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
        catch (OpenSearchStatusException e) {
            Throwable[] suppressed = e.getSuppressed();
            if (suppressed.length > 0) {
                Throwable cause = suppressed[0];
                if (cause instanceof ResponseException responseException) {
                    throw propagate(responseException);
                }
            }

            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            searchStats.add(Duration.nanosSince(start));
        }
    }

    public SearchResponse nextPage(String scrollId)
    {
        LOG.debug("Next page: %s", scrollId);

        SearchScrollRequest request = new SearchScrollRequest(scrollId)
                .scroll(new TimeValue(scrollTimeout.toMillis()));

        long start = System.nanoTime();
        try {
            return client.searchScroll(request);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
        finally {
            nextPageStats.add(Duration.nanosSince(start));
        }
    }

    public long count(String index, int shard, QueryBuilder query)
    {
        SearchSourceBuilder sourceBuilder = SearchSourceBuilder.searchSource()
                .query(query);

        LOG.debug("Count: %s:%s, query: %s", index, shard, sourceBuilder);

        long start = System.nanoTime();
        try {
            Response response;
            try {
                response = client.getLowLevelClient()
                        .performRequest(
                                "GET",
                                format("/%s/_count?preference=_shards:%s", index, shard),
                                ImmutableMap.of(),
                                new StringEntity(sourceBuilder.toString(), APPLICATION_JSON),
                                new BasicHeader("Content-Type", "application/json"));
            }
            catch (ResponseException e) {
                throw propagate(e);
            }
            catch (IOException e) {
                throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
            }

            try {
                return COUNT_RESPONSE_CODEC.fromJson(response.getEntity().getContent())
                        .getCount();
            }
            catch (IOException e) {
                throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
            }
        }
        finally {
            countStats.add(Duration.nanosSince(start));
        }
    }

    public void clearScroll(String scrollId)
    {
        ClearScrollRequest request = new ClearScrollRequest();
        request.addScrollId(scrollId);
        try {
            client.clearScroll(request);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }
    }

    @Managed
    @Nested
    public TimeStat getSearchStats()
    {
        return searchStats;
    }

    @Managed
    @Nested
    public TimeStat getNextPageStats()
    {
        return nextPageStats;
    }

    @Managed
    @Nested
    public TimeStat getCountStats()
    {
        return countStats;
    }

    @Managed
    @Nested
    public TimeStat getBackpressureStats()
    {
        return backpressureStats;
    }

    private <T> T doRequest(String path, ResponseHandler<T> handler)
    {
        checkArgument(path.startsWith("/"), "path must be an absolute path");

        Response response;
        try {
            response = client.getLowLevelClient()
                    .performRequest("GET", path);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_CONNECTION_ERROR, e);
        }

        try (InputStream stream = response.getEntity().getContent()) {
            return handler.process(stream);
        }
        catch (IOException e) {
            throw new TrinoException(OPENSEARCH_INVALID_RESPONSE, e);
        }
    }

    private static TrinoException propagate(ResponseException exception)
    {
        HttpEntity entity = exception.getResponse().getEntity();

        if (entity != null && entity.getContentType() != null) {
            try {
                JsonNode reason = JSON_MAPPER.readTree(entity.getContent()).path("error")
                        .path("root_cause")
                        .path(0)
                        .path("reason");

                if (!reason.isMissingNode()) {
                    throw new TrinoException(OPENSEARCH_QUERY_FAILURE, reason.asText(), exception);
                }
            }
            catch (IOException e) {
                TrinoException result = new TrinoException(OPENSEARCH_QUERY_FAILURE, exception);
                result.addSuppressed(e);
                throw result;
            }
        }

        throw new TrinoException(OPENSEARCH_QUERY_FAILURE, exception);
    }

    @VisibleForTesting
    static Optional<String> extractAddress(String address)
    {
        Matcher matcher = ADDRESS_PATTERN.matcher(address);

        if (!matcher.matches()) {
            return Optional.empty();
        }

        String cname = matcher.group("cname");
        String ip = matcher.group("ip");
        String port = matcher.group("port");

        if (cname != null) {
            return Optional.of(cname + ":" + port);
        }

        return Optional.of(ip + ":" + port);
    }

    private interface ResponseHandler<T>
    {
        T process(InputStream inputStream);
    }
}
