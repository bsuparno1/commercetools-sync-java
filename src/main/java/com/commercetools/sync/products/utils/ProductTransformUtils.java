package com.commercetools.sync.products.utils;

import static com.commercetools.sync.commons.utils.ResourceIdentifierUtils.REFERENCE_ID_FIELD;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import com.commercetools.api.client.ByProjectKeyCustomObjectsGet;
import com.commercetools.api.client.ProjectApiRoot;
import com.commercetools.api.models.category.CategoryReference;
import com.commercetools.api.models.channel.ChannelReference;
import com.commercetools.api.models.common.Asset;
import com.commercetools.api.models.common.Price;
import com.commercetools.api.models.customer_group.CustomerGroupReference;
import com.commercetools.api.models.custom_object.CustomObjectPagedQueryResponse;
import com.commercetools.api.models.graph_ql.GraphQLRequest;
import com.commercetools.api.models.product.Attribute;
import com.commercetools.api.models.product.ProductDraft;
import com.commercetools.api.models.product.ProductDraftBuilder;
import com.commercetools.api.models.product.ProductProjection;
import com.commercetools.api.models.product.ProductVariant;
import com.commercetools.api.models.product_type.ProductTypeReference;
import com.commercetools.api.models.state.StateReference;
import com.commercetools.api.models.tax_category.TaxCategoryReference;
import com.commercetools.api.models.type.CustomFields;
import com.commercetools.api.models.type.TypeReference;
import com.commercetools.sync.commons.exceptions.ReferenceTransformException;
import com.commercetools.sync.commons.models.GraphQlQueryResource;
import com.commercetools.sync.commons.utils.ChunkUtils;
import com.commercetools.sync.commons.utils.ReferenceIdToKeyCache;
import com.commercetools.sync.customobjects.helpers.CustomObjectCompositeIdentifier;
import com.commercetools.sync.services.impl.BaseTransformServiceImpl;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class ProductTransformUtils {

    /**
     * Only used for cleanup (we don't want to persist "key" alongside "id").
     * Actual reference resolution relies on REFERENCE_ID_FIELD being temporarily set to the key.
     */
    private static final String REFERENCE_KEY_FIELD = "key";

    @Nonnull
    public static CompletableFuture<List<ProductDraft>> toProductDrafts(
            @Nonnull final ProjectApiRoot client,
            @Nonnull final ReferenceIdToKeyCache referenceIdToKeyCache,
            @Nonnull final List<ProductProjection> products) {

        final ProductTransformServiceImpl productTransformService =
                new ProductTransformServiceImpl(client, referenceIdToKeyCache);
        return productTransformService.toProductDrafts(products);
    }

    private static List<Attribute> mergeAttributesForCreate(
            @Nullable final List<Attribute> existingDraftAttrs, @Nullable final List<Attribute> sourceProductLevelAttrs) {

        final Map<String, Attribute> merged = new LinkedHashMap<>();

        if (existingDraftAttrs != null) {
            for (Attribute attr : existingDraftAttrs) {
                if (attr != null && attr.getName() != null) {
                    merged.put(attr.getName(), attr);
                }
            }
        }

        if (sourceProductLevelAttrs != null) {
            for (Attribute attr : sourceProductLevelAttrs) {
                if (attr != null && attr.getName() != null) {
                    // product-level attributes override if same name
                    merged.put(attr.getName(), attr);
                }
            }
        }

        return new ArrayList<>(merged.values());
    }

    private static final class ProductTransformServiceImpl extends BaseTransformServiceImpl {

        private static final String FAILED_TO_REPLACE_REFERENCES_ON_ATTRIBUTES =
                "Failed to replace referenced resource ids with keys on the attributes of the products in "
                        + "the current fetched page from the source project. This page will not be synced to the target "
                        + "project.";

        ProductTransformServiceImpl(
                @Nonnull final ProjectApiRoot ctpClient, @Nonnull final ReferenceIdToKeyCache referenceIdToKeyCache) {
            super(ctpClient, referenceIdToKeyCache);
        }

        @Nonnull
        CompletableFuture<List<ProductDraft>> toProductDrafts(@Nonnull final List<ProductProjection> products) {
            return replaceAttributeReferenceIdsWithKeys(products)
                    .handle(
                            (productsResolved, throwable) -> {
                                if (throwable != null) {
                                    throw new ReferenceTransformException(FAILED_TO_REPLACE_REFERENCES_ON_ATTRIBUTES, throwable);
                                }
                                return productsResolved;
                            })
                    .thenCompose(this::transformReferencesAndMapToProductDrafts)
                    .toCompletableFuture();
        }

        @Nonnull
        private CompletionStage<List<ProductDraft>> transformReferencesAndMapToProductDrafts(
                @Nonnull final List<ProductProjection> products) {

            final List<CompletableFuture<Void>> transformReferencesToRunParallel = new ArrayList<>();
            transformReferencesToRunParallel.add(this.transformProductTypeReference(products));
            transformReferencesToRunParallel.add(this.transformTaxCategoryReference(products));
            transformReferencesToRunParallel.add(this.transformStateReference(products));
            transformReferencesToRunParallel.add(this.transformCategoryReference(products));
            transformReferencesToRunParallel.add(this.transformPricesChannelReference(products));
            transformReferencesToRunParallel.add(this.transformCustomTypeReference(products));
            transformReferencesToRunParallel.add(this.transformPricesCustomerGroupReference(products));

            return CompletableFuture.allOf(transformReferencesToRunParallel.toArray(new CompletableFuture[0]))
                    .thenApply(
                            ignore -> {
                                final List<ProductDraft> drafts =
                                        ProductReferenceResolutionUtils.mapToProductDrafts(products, this.referenceIdToKeyCache);
                                return applyProductLevelAttributes(products, drafts);
                            });
        }

        /**
         * IMPORTANT:
         * - Product-level attributes containing product references cannot be safely created/updated by sync-java 10.4.x
         *   when references are temporarily represented as id=key.
         * - We strip them here and rely on Step-2 reconciler to populate/fix them later.
         */
        @Nonnull
        private List<ProductDraft> applyProductLevelAttributes(
                @Nonnull final List<ProductProjection> projections, @Nonnull final List<ProductDraft> drafts) {

            final Map<String, ProductProjection> projectionByKey =
                    projections.stream()
                            .filter(Objects::nonNull)
                            .filter(p -> p.getKey() != null)
                            .collect(Collectors.toMap(ProductProjection::getKey, p -> p, (a, b) -> a));

            final List<ProductDraft> result = new ArrayList<>(drafts.size());

            for (ProductDraft draft : drafts) {
                if (draft == null || draft.getKey() == null) {
                    result.add(draft);
                    continue;
                }

                final ProductProjection projection = projectionByKey.get(draft.getKey());
                final List<Attribute> projectionProductAttrs =
                        projection != null ? projection.getAttributes() : null;

                if (projectionProductAttrs == null || projectionProductAttrs.isEmpty()) {
                    result.add(draft);
                    continue;
                }

                final List<Attribute> merged = mergeAttributesForCreate(draft.getAttributes(), projectionProductAttrs);

                final List<Attribute> safeForCreate =
                        merged.stream().filter(a -> !isProductReferenceAttribute(a)).collect(Collectors.toList());

                result.add(ProductDraftBuilder.of(draft).attributes(safeForCreate).build());
            }

            return result;
        }

        @Nonnull
        private CompletableFuture<Void> transformProductTypeReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> productTypeIds =
                    products.stream().map(ProductProjection::getProductType).map(ProductTypeReference::getId).collect(toSet());
            return fetchAndFillReferenceIdToKeyCache(productTypeIds, GraphQlQueryResource.PRODUCT_TYPES);
        }

        @Nonnull
        private CompletableFuture<Void> transformTaxCategoryReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> taxCategoryIds =
                    products.stream()
                            .map(ProductProjection::getTaxCategory)
                            .filter(Objects::nonNull)
                            .map(TaxCategoryReference::getId)
                            .collect(toSet());
            return fetchAndFillReferenceIdToKeyCache(taxCategoryIds, GraphQlQueryResource.TAX_CATEGORIES);
        }

        @Nonnull
        private CompletableFuture<Void> transformStateReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> stateIds =
                    products.stream()
                            .map(ProductProjection::getState)
                            .filter(Objects::nonNull)
                            .map(StateReference::getId)
                            .collect(toSet());
            return fetchAndFillReferenceIdToKeyCache(stateIds, GraphQlQueryResource.STATES);
        }

        @Nonnull
        private CompletableFuture<Void> transformCategoryReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> categoryIds =
                    products.stream()
                            .map(ProductProjection::getCategories)
                            .filter(Objects::nonNull)
                            .map(categories -> categories.stream().map(CategoryReference::getId).collect(Collectors.toList()))
                            .flatMap(Collection::stream)
                            .collect(toSet());
            return fetchAndFillReferenceIdToKeyCache(categoryIds, GraphQlQueryResource.CATEGORIES);
        }

        @Nonnull
        private CompletableFuture<Void> transformPricesChannelReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> channelIds =
                    products.stream()
                            .map(ProductProjection::getAllVariants)
                            .map(
                                    productVariants ->
                                            productVariants.stream()
                                                    .filter(Objects::nonNull)
                                                    .map(
                                                            productVariant ->
                                                                    productVariant.getPrices().stream()
                                                                            .map(Price::getChannel)
                                                                            .filter(Objects::nonNull)
                                                                            .map(ChannelReference::getId)
                                                                            .collect(toList()))
                                                    .flatMap(Collection::stream)
                                                    .collect(toList()))
                            .flatMap(Collection::stream)
                            .collect(toSet());

            return fetchAndFillReferenceIdToKeyCache(channelIds, GraphQlQueryResource.CHANNELS);
        }

        @Nonnull
        private CompletableFuture<Void> transformCustomTypeReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> typeIds = new HashSet<>();
            typeIds.addAll(collectPriceCustomTypeIds(products));
            typeIds.addAll(collectAssetCustomTypeIds(products));
            return fetchAndFillReferenceIdToKeyCache(typeIds, GraphQlQueryResource.TYPES);
        }

        private Set<String> collectPriceCustomTypeIds(@Nonnull final List<ProductProjection> products) {
            return products.stream()
                    .map(ProductProjection::getAllVariants)
                    .map(
                            productVariants ->
                                    productVariants.stream()
                                            .filter(Objects::nonNull)
                                            .map(
                                                    productVariant ->
                                                            productVariant.getPrices().stream()
                                                                    .map(Price::getCustom)
                                                                    .filter(Objects::nonNull)
                                                                    .map(CustomFields::getType)
                                                                    .map(TypeReference::getId)
                                                                    .collect(toList()))
                                            .flatMap(Collection::stream)
                                            .collect(toList()))
                    .flatMap(Collection::stream)
                    .collect(toSet());
        }

        private Set<String> collectAssetCustomTypeIds(@Nonnull final List<ProductProjection> products) {
            return products.stream()
                    .map(ProductProjection::getAllVariants)
                    .map(
                            productVariants ->
                                    productVariants.stream()
                                            .filter(Objects::nonNull)
                                            .map(
                                                    productVariant ->
                                                            productVariant.getAssets().stream()
                                                                    .map(Asset::getCustom)
                                                                    .filter(Objects::nonNull)
                                                                    .map(CustomFields::getType)
                                                                    .map(TypeReference::getId)
                                                                    .collect(toList()))
                                            .flatMap(Collection::stream)
                                            .collect(toList()))
                    .flatMap(Collection::stream)
                    .collect(toSet());
        }

        @Nonnull
        private CompletableFuture<Void> transformPricesCustomerGroupReference(@Nonnull final List<ProductProjection> products) {
            final Set<String> customerGroupIds =
                    products.stream()
                            .map(ProductProjection::getAllVariants)
                            .map(
                                    productVariants ->
                                            productVariants.stream()
                                                    .filter(Objects::nonNull)
                                                    .map(
                                                            productVariant ->
                                                                    productVariant.getPrices().stream()
                                                                            .map(Price::getCustomerGroup)
                                                                            .filter(Objects::nonNull)
                                                                            .map(CustomerGroupReference::getId)
                                                                            .collect(toList()))
                                                    .flatMap(Collection::stream)
                                                    .collect(toList()))
                            .flatMap(Collection::stream)
                            .collect(toSet());

            return fetchAndFillReferenceIdToKeyCache(customerGroupIds, GraphQlQueryResource.CUSTOMER_GROUPS);
        }

        @Nonnull
        public CompletionStage<List<ProductProjection>> replaceAttributeReferenceIdsWithKeys(
                @Nonnull final List<ProductProjection> products) {

            final List<JsonNode> allAttributeReferences = getAllReferences(products);
            return getIdToKeys(allAttributeReferences)
                    .thenApply(
                            ignored -> {
                                replaceReferences(getAllReferences(products));
                                return products;
                            });
        }

        @Nonnull
        private List<JsonNode> getAllReferences(@Nonnull final List<ProductProjection> products) {
            return products.stream().map(this::getAllReferences).flatMap(Collection::stream).collect(toList());
        }

        private List<JsonNode> getAllReferences(@Nonnull final ProductProjection product) {
            final List<JsonNode> refs = new ArrayList<>();

            final List<ProductVariant> allVariants = product.getAllVariants();
            refs.addAll(getAttributeReferences(allVariants));

            final List<Attribute> productLevelAttrs = product.getAttributes();
            if (productLevelAttrs != null && !productLevelAttrs.isEmpty()) {
                refs.addAll(getProductLevelAttributeReferences(productLevelAttrs));
            }

            return refs;
        }

        @Nonnull
        private List<JsonNode> getProductLevelAttributeReferences(@Nonnull final List<Attribute> attrs) {
            return attrs.stream()
                    .map(AttributeUtils::replaceAttributeValueWithJsonAndReturnValue)
                    .map(AttributeUtils::getAttributeReferences)
                    .flatMap(Collection::stream)
                    .collect(toList());
        }

        @Nonnull
        private List<JsonNode> getAttributeReferences(@Nonnull final List<ProductVariant> variants) {
            return variants.stream()
                    .map(ProductVariant::getAttributes)
                    .flatMap(Collection::stream)
                    .map(AttributeUtils::replaceAttributeValueWithJsonAndReturnValue)
                    .map(AttributeUtils::getAttributeReferences)
                    .flatMap(Collection::stream)
                    .collect(toList());
        }

        private void replaceReferences(@Nonnull final List<JsonNode> allAttributeReferences) {
            allAttributeReferences.forEach(
                    reference -> {
                        if (!(reference instanceof ObjectNode)) {
                            return;
                        }
                        final ObjectNode refObj = (ObjectNode) reference;

                        final JsonNode idNode = refObj.get(REFERENCE_ID_FIELD);
                        if (idNode == null || idNode.isNull()) {
                            return;
                        }

                        final String sourceId = idNode.asText();
                        final String key = referenceIdToKeyCache.get(sourceId);
                        if (key == null || key.trim().isEmpty()) {
                            return;
                        }

                        // CRITICAL: resolver expects "id" to temporarily contain the key.
                        refObj.put(REFERENCE_ID_FIELD, key);
                        refObj.remove(REFERENCE_KEY_FIELD);
                    });
        }

        @Nonnull
        CompletableFuture<Void> getIdToKeys(@Nonnull final List<JsonNode> allAttributeReferences) {

            final Set<JsonNode> nonCachedReferences = getNonCachedReferences(allAttributeReferences);
            final Map<GraphQlQueryResource, Set<String>> map =
                    buildMapOfRequestTypeToReferencedIds(nonCachedReferences);

            final Set<String> nonCachedCustomObjectIds = map.remove(GraphQlQueryResource.CUSTOM_OBJECTS);

            if (map.values().isEmpty() || map.values().stream().allMatch(Set::isEmpty)) {
                return fetchCustomObjectKeys(nonCachedCustomObjectIds);
            }

            final List<GraphQLRequest> collectedRequests =
                    map.keySet().stream()
                            .map(
                                    resource -> {
                                        final List<List<String>> chunk = ChunkUtils.chunk(map.get(resource), CHUNK_SIZE);
                                        return createGraphQLRequests(chunk, resource);
                                    })
                            .flatMap(Collection::stream)
                            .collect(toList());

            return ChunkUtils.executeChunks(getCtpClient(), collectedRequests)
                    .thenAccept(this::cacheResourceReferenceKeys)
                    .thenCompose(ignored -> fetchCustomObjectKeys(nonCachedCustomObjectIds));
        }

        @Nonnull
        private CompletableFuture<Void> fetchCustomObjectKeys(@Nullable final Set<String> nonCachedCustomObjectIds) {
            if (nonCachedCustomObjectIds == null || nonCachedCustomObjectIds.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }

            final List<List<String>> chunkedIds = ChunkUtils.chunk(nonCachedCustomObjectIds, CHUNK_SIZE);

            final List<ByProjectKeyCustomObjectsGet> chunkedRequests =
                    chunkedIds.stream()
                            .map(
                                    ids ->
                                            getCtpClient()
                                                    .customObjects()
                                                    .get()
                                                    .withWhere("id in :ids")
                                                    .withPredicateVar("ids", ids)
                                                    .withLimit(CHUNK_SIZE)
                                                    .withWithTotal(false))
                            .collect(toList());

            return ChunkUtils.executeChunks(chunkedRequests)
                    .thenAccept(
                            chunk ->
                                    chunk.forEach(
                                            response -> {
                                                final CustomObjectPagedQueryResponse responseBody = response.getBody();
                                                responseBody
                                                        .getResults()
                                                        .forEach(
                                                                customObject ->
                                                                        referenceIdToKeyCache.add(
                                                                                customObject.getId(),
                                                                                CustomObjectCompositeIdentifier.of(customObject).toString()));
                                            }));
        }
    }

    private static boolean isProductReferenceAttribute(@Nullable final Attribute attr) {
        if (attr == null) return false;
        return AttributeUtils.getAttributeReferences(AttributeUtils.replaceAttributeValueWithJsonAndReturnValue(attr))
                .stream()
                .anyMatch(ref -> "product".equals(ref.get("typeId").asText()));
    }

    private ProductTransformUtils() {}
}