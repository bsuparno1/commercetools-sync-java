package com.commercetools.sync.products;

import com.commercetools.api.client.ProjectApiRoot;
import com.commercetools.api.models.channel.ChannelRoleEnum;
import com.commercetools.api.models.custom_object.CustomObject;
import com.commercetools.api.models.custom_object.CustomObjectDraft;
import com.commercetools.api.models.custom_object.CustomObjectDraftBuilder;
import com.commercetools.api.models.product.Attribute;
import com.commercetools.api.models.product.AttributeBuilder;
import com.commercetools.api.models.product.Product;
import com.commercetools.api.models.product.ProductDraft;
import com.commercetools.api.models.product.ProductDraftBuilder;
import com.commercetools.api.models.product.ProductProjection;
import com.commercetools.api.models.product.ProductResourceIdentifierBuilder;
import com.commercetools.api.models.product.ProductSetAttributeActionBuilder;
import com.commercetools.api.models.product.ProductUpdateAction;
import com.commercetools.api.models.product.ProductVariantDraft;
import com.commercetools.api.models.product.ProductVariantDraftBuilder;
import com.commercetools.sync.categories.CategorySyncOptionsBuilder;
import com.commercetools.sync.commons.BaseSync;
import com.commercetools.sync.commons.exceptions.SyncException;
import com.commercetools.sync.commons.models.WaitingToBeResolvedProducts;
import com.commercetools.sync.commons.utils.ReferenceIdToKeyCache;
import com.commercetools.sync.customers.CustomerSyncOptionsBuilder;
import com.commercetools.sync.customobjects.CustomObjectSyncOptionsBuilder;
import com.commercetools.sync.products.helpers.ProductBatchValidator;
import com.commercetools.sync.products.helpers.ProductReferenceResolver;
import com.commercetools.sync.products.helpers.ProductSyncStatistics;
import com.commercetools.sync.services.CategoryService;
import com.commercetools.sync.services.ChannelService;
import com.commercetools.sync.services.CustomObjectService;
import com.commercetools.sync.services.CustomerGroupService;
import com.commercetools.sync.services.CustomerService;
import com.commercetools.sync.services.ProductService;
import com.commercetools.sync.services.ProductTypeService;
import com.commercetools.sync.services.StateService;
import com.commercetools.sync.services.TaxCategoryService;
import com.commercetools.sync.services.TypeService;
import com.commercetools.sync.services.UnresolvedReferencesService;
import com.commercetools.sync.services.impl.CategoryServiceImpl;
import com.commercetools.sync.services.impl.ChannelServiceImpl;
import com.commercetools.sync.services.impl.CustomObjectServiceImpl;
import com.commercetools.sync.services.impl.CustomerGroupServiceImpl;
import com.commercetools.sync.services.impl.CustomerServiceImpl;
import com.commercetools.sync.services.impl.ProductServiceImpl;
import com.commercetools.sync.services.impl.ProductTypeServiceImpl;
import com.commercetools.sync.services.impl.StateServiceImpl;
import com.commercetools.sync.services.impl.TaxCategoryServiceImpl;
import com.commercetools.sync.services.impl.TypeServiceImpl;
import com.commercetools.sync.services.impl.UnresolvedReferencesServiceImpl;
import com.commercetools.sync.states.StateSyncOptionsBuilder;
import com.commercetools.sync.taxcategories.TaxCategorySyncOptionsBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.tuple.ImmutablePair;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.commercetools.sync.commons.helpers.BaseReferenceResolver.SELF_REFERENCING_ID_PLACE_HOLDER;
import static com.commercetools.sync.commons.utils.SyncUtils.batchElements;
import static com.commercetools.sync.products.utils.ProductSyncUtils.buildActions;
import static com.commercetools.sync.products.utils.ProductUpdateActionUtils.getAllVariants;
import static java.lang.String.format;
import static java.util.Collections.singleton;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ProductSync
    extends BaseSync<
        ProductProjection,
        ProductDraft,
        ProductUpdateAction,
        ProductSyncStatistics,
        ProductSyncOptions> {
  private static final String CTP_PRODUCT_FETCH_FAILED =
      "Failed to fetch existing products with keys: '%s'.";
  private static final String UNRESOLVED_REFERENCES_STORE_FETCH_FAILED =
      "Failed to fetch ProductDrafts waiting to be resolved with keys '%s'.";
  private static final String UPDATE_FAILED = "Failed to update Product with key: '%s'. Reason: %s";
  private static final String FAILED_TO_PROCESS =
      "Failed to process the ProductDraft with key:'%s'. Reason: %s";
  private static final String FAILED_TO_FETCH_PRODUCT_TYPE =
      "Failed to fetch a productType for the product to "
          + "build the products' attributes metadata.";

    private static final String CUSTOM_OBJECT_DEFERRED_ATTRS = "deferred-product-attributes";
    private static final String DEFERRED_VARIANT_ATTRS_CONTAINER = "commercetools-project-sync.deferred-variant-attrs";
    private static final String DEFERRED_MASTER_VARIANT_ATTRS_CONTAINER = "commercetools-project-sync.deferred-master-variant-attrs";

  private final ProductService productService;
  private final ProductTypeService productTypeService;
  private final ProductReferenceResolver productReferenceResolver;
  private final UnresolvedReferencesService<WaitingToBeResolvedProducts>
      unresolvedReferencesService;
  private final ProductBatchValidator batchValidator;

  private ConcurrentHashMap.KeySetView<String, Boolean> readyToResolve;

  /**
   * Takes a {@link ProductSyncOptions} instance to instantiate a new {@link ProductSync} instance
   * that could be used to sync ProductProjection drafts with the given products in the CTP project
   * specified in the injected {@link ProductSyncOptions} instance.
   *
   * @param productSyncOptions the container of all the options of the sync process including the
   *     CTP project client and/or configuration and other sync-specific options.
   */
  public ProductSync(@Nonnull final ProductSyncOptions productSyncOptions) {
    this(
        productSyncOptions,
        new ProductServiceImpl(productSyncOptions),
        new ProductTypeServiceImpl(productSyncOptions),
        new CategoryServiceImpl(
            CategorySyncOptionsBuilder.of(productSyncOptions.getCtpClient()).build()),
        new TypeServiceImpl(productSyncOptions),
        new ChannelServiceImpl(productSyncOptions, singleton(ChannelRoleEnum.PRODUCT_DISTRIBUTION)),
        new CustomerGroupServiceImpl(productSyncOptions),
        new TaxCategoryServiceImpl(
            TaxCategorySyncOptionsBuilder.of(productSyncOptions.getCtpClient()).build()),
        new StateServiceImpl(StateSyncOptionsBuilder.of(productSyncOptions.getCtpClient()).build()),
        new UnresolvedReferencesServiceImpl<>(productSyncOptions),
        new CustomObjectServiceImpl(
            CustomObjectSyncOptionsBuilder.of(productSyncOptions.getCtpClient()).build()),
        new CustomerServiceImpl(
            CustomerSyncOptionsBuilder.of(productSyncOptions.getCtpClient()).build()));
  }

  ProductSync(
      @Nonnull final ProductSyncOptions productSyncOptions,
      @Nonnull final ProductService productService,
      @Nonnull final ProductTypeService productTypeService,
      @Nonnull final CategoryService categoryService,
      @Nonnull final TypeService typeService,
      @Nonnull final ChannelService channelService,
      @Nonnull final CustomerGroupService customerGroupService,
      @Nonnull final TaxCategoryService taxCategoryService,
      @Nonnull final StateService stateService,
      @Nonnull
          final UnresolvedReferencesService<WaitingToBeResolvedProducts>
              unresolvedReferencesService,
      @Nonnull final CustomObjectService customObjectService,
      @Nonnull final CustomerService customerService) {
    super(new ProductSyncStatistics(), productSyncOptions);
    this.productService = productService;
    this.productTypeService = productTypeService;
    this.productReferenceResolver =
        new ProductReferenceResolver(
            getSyncOptions(),
            productTypeService,
            categoryService,
            typeService,
            channelService,
            customerGroupService,
            taxCategoryService,
            stateService,
            productService,
            customObjectService,
            customerService);
    this.unresolvedReferencesService = unresolvedReferencesService;
    this.batchValidator = new ProductBatchValidator(getSyncOptions(), getStatistics());
  }

  @Override
  protected CompletionStage<ProductSyncStatistics> process(
      @Nonnull final List<ProductDraft> resourceDrafts) {
    final List<List<ProductDraft>> batches =
        batchElements(resourceDrafts, syncOptions.getBatchSize());
    return syncBatches(batches, CompletableFuture.completedFuture(statistics));
  }

  @Override
  protected CompletionStage<ProductSyncStatistics> processBatch(
      @Nonnull final List<ProductDraft> batch) {

    readyToResolve = ConcurrentHashMap.newKeySet();

    final ImmutablePair<Set<ProductDraft>, ProductBatchValidator.ReferencedKeys> result =
        batchValidator.validateAndCollectReferencedKeys(batch);

    final Set<ProductDraft> validDrafts = result.getLeft();
    if (validDrafts.isEmpty()) {
      statistics.incrementProcessed(batch.size());
      return CompletableFuture.completedFuture(statistics);
    }

    return productReferenceResolver
        .populateKeyToIdCachesForReferencedKeys(result.getRight())
        .handle(ImmutablePair::new)
        .thenCompose(
            cachingResponse -> {
              final Throwable cachingException = cachingResponse.getValue();
              if (cachingException != null) {
                handleError(
                    "Failed to build a cache of keys to ids.",
                    cachingException,
                    null,
                    null,
                    null,
                    validDrafts.size());
                return CompletableFuture.completedFuture(null);
              }

              final Map<String, String> productKeyToIdCache = cachingResponse.getKey();
              return syncBatch(validDrafts, productKeyToIdCache);
            })
        .thenApply(
            ignoredResult -> {
              statistics.incrementProcessed(batch.size());
              return statistics;
            });
  }

  @Nonnull
  private CompletionStage<Void> syncBatch(
      @Nonnull final Set<ProductDraft> productDrafts,
      @Nonnull final Map<String, String> keyToIdCache) {

    if (productDrafts.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    final Set<String> productDraftKeys =
        productDrafts.stream().map(ProductDraft::getKey).collect(Collectors.toSet());

    return productService
        .fetchMatchingProductsByKeys(productDraftKeys)
        .handle(ImmutablePair::new)
        .thenCompose(
            fetchResponse -> {
              final Throwable fetchException = fetchResponse.getValue();
              if (fetchException != null) {
                final String errorMessage = format(CTP_PRODUCT_FETCH_FAILED, productDraftKeys);
                handleError(
                    errorMessage, fetchException, null, null, null, productDraftKeys.size());
                return CompletableFuture.completedFuture(null);
              } else {
                final Set<ProductProjection> matchingProducts = fetchResponse.getKey();
                return syncOrKeepTrack(productDrafts, matchingProducts, keyToIdCache)
                    .thenCompose(aVoid -> resolveNowReadyReferences(keyToIdCache));
              }
            });
  }

  /**
   * Given a set of ProductProjection drafts, for each new draft: if it doesn't have any
   * ProductProjection references which are missing, it syncs the new draft. However, if it does
   * have missing references, it keeps track of it by persisting it.
   *
   * @param oldProducts old ProductProjection types.
   * @param newProducts drafts that need to be synced.
   * @return a {@link java.util.concurrent.CompletionStage} which contains an empty result after
   *     execution of the update
   */
  @Nonnull
  private CompletionStage<Void> syncOrKeepTrack(
      @Nonnull final Set<ProductDraft> newProducts,
      @Nonnull final Set<ProductProjection> oldProducts,
      @Nonnull final Map<String, String> keyToIdCache) {

    return allOf(
        newProducts.stream()
            .map(
                newDraft -> {
                  final Set<String> missingReferencedProductKeys =
                      getMissingReferencedProductKeys(newDraft, keyToIdCache);

                  final boolean selfReferenceExists =
                      missingReferencedProductKeys.remove(newDraft.getKey());

                  if (!missingReferencedProductKeys.isEmpty()) {
                    return keepTrackOfMissingReferences(newDraft, missingReferencedProductKeys);
                  } else if (selfReferenceExists) {
                    keyToIdCache.put(newDraft.getKey(), SELF_REFERENCING_ID_PLACE_HOLDER);
                    return keepTrackOfMissingReferences(newDraft, singleton(newDraft.getKey()))
                        .thenCompose(optional -> syncDraft(oldProducts, newDraft));
                  } else {
                    return syncDraft(oldProducts, newDraft);
                  }
                })
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new));
  }

  private Set<String> getMissingReferencedProductKeys(
      @Nonnull final ProductDraft newProduct, @Nonnull final Map<String, String> keyToIdCache) {

    final Set<String> referencedProductKeys =
        getAllVariants(newProduct).stream()
            .map(ProductBatchValidator::getReferencedProductKeys)
            .flatMap(Collection::stream)
            .collect(Collectors.toSet());

    return referencedProductKeys.stream()
        .filter(key -> !keyToIdCache.containsKey(key))
        .collect(Collectors.toSet());
  }

  private CompletionStage<Optional<WaitingToBeResolvedProducts>> keepTrackOfMissingReferences(
      @Nonnull final ProductDraft newProduct,
      @Nonnull final Set<String> missingReferencedProductKeys) {

    missingReferencedProductKeys.forEach(
        missingParentKey -> statistics.addMissingDependency(missingParentKey, newProduct.getKey()));
    return unresolvedReferencesService.save(
        new WaitingToBeResolvedProducts(newProduct, missingReferencedProductKeys),
        UnresolvedReferencesServiceImpl.CUSTOM_OBJECT_PRODUCT_CONTAINER_KEY,
        WaitingToBeResolvedProducts.class);
  }

  @Nonnull
  private CompletionStage<Void> resolveNowReadyReferences(final Map<String, String> keyToIdCache) {

    final Set<String> referencingDraftKeys =
        readyToResolve.stream()
            .map(statistics::removeAndGetReferencingKeys)
            .filter(Objects::nonNull)
            .flatMap(Set::stream)
            .collect(Collectors.toSet());

    if (referencingDraftKeys.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    final Set<ProductDraft> readyToSync = new HashSet<>();
    final Set<WaitingToBeResolvedProducts> waitingDraftsToBeUpdated = new HashSet<>();

    return unresolvedReferencesService
        .fetch(
            referencingDraftKeys,
            UnresolvedReferencesServiceImpl.CUSTOM_OBJECT_PRODUCT_CONTAINER_KEY,
            WaitingToBeResolvedProducts.class)
        .handle(ImmutablePair::new)
        .thenCompose(
            fetchResponse -> {
              final Set<WaitingToBeResolvedProducts> waitingDrafts = fetchResponse.getKey();
              final Throwable fetchException = fetchResponse.getValue();

              if (fetchException != null) {
                final String errorMessage =
                    format(UNRESOLVED_REFERENCES_STORE_FETCH_FAILED, referencingDraftKeys);
                handleError(
                    errorMessage, fetchException, null, null, null, referencingDraftKeys.size());
                return CompletableFuture.completedFuture(null);
              }

              waitingDrafts.forEach(
                  waitingDraft -> {
                    final Set<String> missingReferencedProductKeys =
                        waitingDraft.getMissingReferencedProductKeys();
                    missingReferencedProductKeys.removeAll(readyToResolve);

                    if (missingReferencedProductKeys.isEmpty()) {
                      readyToSync.add(waitingDraft.getProductDraft());
                    } else {
                      waitingDraftsToBeUpdated.add(waitingDraft);
                    }
                  });

                return updateWaitingDrafts(waitingDraftsToBeUpdated)
                        .thenCompose(aVoid -> syncBatch(readyToSync, keyToIdCache))
                        .thenCompose(aVoid -> applyDeferredAttributesForSyncedProducts(readyToSync, keyToIdCache))
                        .thenCompose(aVoid ->
                                CompletableFuture.allOf(
                                        readyToSync.stream()
                                                .map(ProductDraft::getKey)
                                                .map(productKey ->
                                                        unresolvedReferencesService
                                                                .fetch(
                                                                        Set.of(productKey),
                                                                        UnresolvedReferencesServiceImpl.CUSTOM_OBJECT_PRODUCT_CONTAINER_KEY,
                                                                        WaitingToBeResolvedProducts.class
                                                                )
                                                                .thenCompose(waitingSet -> {
                                                                    if (waitingSet == null || waitingSet.isEmpty()) {
                                                                        return CompletableFuture.completedFuture(null);
                                                                    }

                                                                    final WaitingToBeResolvedProducts waiting =
                                                                            waitingSet.iterator().next();

                                                                    final ProductDraft deferredDraft =
                                                                            waiting.getProductDraft();

                                                                    // Resolve references NOW that products exist
                                                                    return productReferenceResolver
                                                                            .resolveReferences(deferredDraft)
                                                                            .thenCompose(resolvedDraft ->
                                                                                    productService
                                                                                            .fetchProduct(productKey)
                                                                                            .thenCompose(optProduct -> {
                                                                                                if (!optProduct.isPresent()) {
                                                                                                    return CompletableFuture.completedFuture(null);
                                                                                                }

                                                                                                final ProductProjection product =
                                                                                                        optProduct.get();

                                                                                                return fetchProductAttributesMetadataAndUpdate(
                                                                                                        product,
                                                                                                        resolvedDraft
                                                                                                );
                                                                                            })
                                                                            )
                                                                            .thenCompose(ignored ->
                                                                                    unresolvedReferencesService.delete(
                                                                                            productKey,
                                                                                            UnresolvedReferencesServiceImpl
                                                                                                    .CUSTOM_OBJECT_PRODUCT_CONTAINER_KEY,
                                                                                            WaitingToBeResolvedProducts.class
                                                                                    )
                                                                            );
                                                                })
                                                )

                                                .map(CompletionStage::toCompletableFuture)
                                                .toArray(CompletableFuture[]::new)
                                )
                        )
                        .thenCompose(aVoid -> removeFromWaiting(readyToSync));

            });
  }

  @Nonnull
  private CompletableFuture<Void> updateWaitingDrafts(
      @Nonnull final Set<WaitingToBeResolvedProducts> waitingDraftsToBeUpdated) {

    return allOf(
        waitingDraftsToBeUpdated.stream()
            .map(
                draft ->
                    unresolvedReferencesService.save(
                        draft,
                        UnresolvedReferencesServiceImpl.CUSTOM_OBJECT_PRODUCT_CONTAINER_KEY,
                        WaitingToBeResolvedProducts.class))
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new));
  }

  @Nonnull
  private CompletableFuture<Void> removeFromWaiting(@Nonnull final Set<ProductDraft> drafts) {
    return allOf(
        drafts.stream()
            .map(ProductDraft::getKey)
            .map(
                key ->
                    unresolvedReferencesService.delete(
                        key,
                        UnresolvedReferencesServiceImpl.CUSTOM_OBJECT_PRODUCT_CONTAINER_KEY,
                        WaitingToBeResolvedProducts.class))
            .map(CompletionStage::toCompletableFuture)
            .toArray(CompletableFuture[]::new));
  }

  @Nonnull
  private CompletionStage<Void> syncDraft(
      @Nonnull final Set<ProductProjection> oldProducts,
      @Nonnull final ProductDraft newProductDraft) {

    final Map<String, ProductProjection> oldProductMap =
        oldProducts.stream().collect(toMap(ProductProjection::getKey, identity()));

    return productReferenceResolver
        .resolveReferences(newProductDraft)
        .thenCompose(
            resolvedDraft -> {
              final ProductProjection oldProduct = oldProductMap.get(newProductDraft.getKey());

              return ofNullable(oldProduct)
                  .map(
                      ProductProjection ->
                          fetchProductAttributesMetadataAndUpdate(oldProduct, resolvedDraft))
                  .orElseGet(() -> applyCallbackAndCreate(resolvedDraft));
            })
        .exceptionally(
            completionException -> {
              final String errorMessage =
                  format(
                      FAILED_TO_PROCESS,
                      newProductDraft.getKey(),
                      completionException.getMessage());
              handleError(errorMessage, completionException, null, null, null, 1);
              return null;
            });
  }

  @Nonnull
  private CompletionStage<Void> fetchProductAttributesMetadataAndUpdate(
      @Nonnull final ProductProjection oldProduct, @Nonnull final ProductDraft newProduct) {

    return productTypeService
        .fetchCachedProductAttributeMetaDataMap(oldProduct.getProductType().getId())
        .thenCompose(
            optionalAttributesMetaDataMap ->
                optionalAttributesMetaDataMap
                    .map(
                        attributeMetaDataMap -> {
                          final List<ProductUpdateAction> updateActions =
                              buildActions(
                                  oldProduct, newProduct, syncOptions, attributeMetaDataMap);

                          final List<ProductUpdateAction> beforeUpdateCallBackApplied =
                              syncOptions.applyBeforeUpdateCallback(
                                  updateActions, newProduct, oldProduct);

                          if (!beforeUpdateCallBackApplied.isEmpty()) {
                            return updateProduct(
                                oldProduct, newProduct, beforeUpdateCallBackApplied);
                          }

                          return CompletableFuture.completedFuture((Void) null);
                        })
                    .orElseGet(
                        () -> {
                          final String errorMessage =
                              format(
                                  UPDATE_FAILED, oldProduct.getKey(), FAILED_TO_FETCH_PRODUCT_TYPE);
                          handleError(errorMessage, null, oldProduct, newProduct, null, 1);
                          return CompletableFuture.completedFuture(null);
                        }));
  }

  @Nonnull
  private CompletionStage<Void> updateProduct(
      @Nonnull final ProductProjection oldProduct,
      @Nonnull final ProductDraft newProduct,
      @Nonnull final List<ProductUpdateAction> updateActions) {

    return productService
        .updateProduct(oldProduct, updateActions)
        .handle(ImmutablePair::new)
        .thenCompose(
            updateResponse -> {
              final Throwable throwable = updateResponse.getValue();
              if (throwable != null) {
                return executeSupplierIfConcurrentModificationException(
                    throwable,
                    () -> fetchAndUpdate(oldProduct, newProduct),
                    () -> {
                      final String productKey = oldProduct.getKey();
                      handleProductSyncError(
                          format(UPDATE_FAILED, productKey, throwable),
                          throwable,
                          oldProduct,
                          newProduct,
                          updateActions);
                      return CompletableFuture.completedFuture(null);
                    });
              } else {
                statistics.incrementUpdated();
                return CompletableFuture.completedFuture(null);
              }
            });
  }

  /**
   * Given an existing {@link Product} and a new {@link ProductDraft}, first fetches a fresh copy of
   * the existing product, then it calculates all the update actions required to synchronize the
   * existing product to be the same as the new one. If there are update actions found, a request is
   * made to CTP to update the existing one, otherwise it doesn't issue a request.
   *
   * @param oldProduct the product which could be updated.
   * @param newProduct the ProductProjection draft where we get the new data.
   * @return a future which contains an empty result after execution of the update.
   */
  @Nonnull
  private CompletionStage<Void> fetchAndUpdate(
      @Nonnull final ProductProjection oldProduct, @Nonnull final ProductDraft newProduct) {

    final String key = oldProduct.getKey();
    return productService
        .fetchProduct(key)
        .handle(ImmutablePair::new)
        .thenCompose(
            fetchResponse -> {
              final Optional<ProductProjection> fetchedProductOptional = fetchResponse.getKey();
              final Throwable exception = fetchResponse.getValue();

              if (exception != null) {
                final String errorMessage =
                    format(
                        UPDATE_FAILED,
                        key,
                        "Failed to fetch from CTP while "
                            + "retrying after concurrency modification.");
                handleError(errorMessage, exception, oldProduct, newProduct, null, 1);
                return CompletableFuture.completedFuture(null);
              }

              return fetchedProductOptional
                  .map(
                      fetchedProduct ->
                          fetchProductAttributesMetadataAndUpdate(fetchedProduct, newProduct))
                  .orElseGet(
                      () -> {
                        final String errorMessage =
                            format(
                                UPDATE_FAILED,
                                key,
                                "Not found when attempting to fetch "
                                    + "while retrying after concurrency modification.");
                        handleError(errorMessage, null, oldProduct, newProduct, null, 1);
                        return CompletableFuture.completedFuture(null);
                      });
            });
  }

  @Nonnull
  private CompletionStage<Void> applyCallbackAndCreate(@Nonnull final ProductDraft productDraft) {

      // 1) split attributes
      final List<Attribute> plainProductLevelAttrs = new ArrayList<>();
      final List<Attribute> deferredProductLevelAttrs = new ArrayList<>();

      final List<Attribute> productLevelAttrs = productDraft.getAttributes();

      if (productLevelAttrs != null) {
          for (Attribute a : productLevelAttrs) {
              if (isProductReferenceAttribute(a)) {
                  deferredProductLevelAttrs.add(a);
              }
              else {
                  plainProductLevelAttrs.add(a);
              }
          }
      }

      // 1b) split MASTER VARIANT attributes  (THIS is what you are missing today)
      final List<Attribute> plainMasterVariantAttrs = new ArrayList<>();
      final List<Attribute> deferredMasterVariantAttrs = new ArrayList<>();

      final List<Attribute> masterVariantAttrs =
              productDraft.getMasterVariant() != null
                      ? productDraft.getMasterVariant().getAttributes()
                      : Collections.emptyList();

      if (masterVariantAttrs != null) {
          for (Attribute a : masterVariantAttrs) {
              if (isProductReferenceAttribute(a)) {

                  deferredMasterVariantAttrs.add(
                          AttributeBuilder.of()
                                  .name(a.getName())
                                  .value(a.getValue())
                                  .build()
                  );

              } else {
                  plainMasterVariantAttrs.add(a);
              }
          }
      }

      final List<ProductVariantDraft> cleanedVariants = new ArrayList<>();
      final List<Attribute> deferredOtherVariantAttrs = new ArrayList<>();

      final List<ProductVariantDraft> variants =
              productDraft.getVariants() != null ? productDraft.getVariants() : Collections.emptyList();

      for (ProductVariantDraft v : variants) {
          if (v == null) continue;

          final List<Attribute> plainAttrs = new ArrayList<>();
          final List<Attribute> attrs = v.getAttributes() != null ? v.getAttributes() : Collections.emptyList();

          for (Attribute a : attrs) {
              if (a == null) continue;

              if (isProductReferenceAttribute(a)) {

                  //Attribute attr = AttributeBuilder.of().name(a.getName()).value(a.getValue()).build();
                  final Map<String, Object> entry = new HashMap<>();
                  entry.put("sku", v.getSku());      // 🔑 THIS is the missing piece
                  entry.put("value", a.getValue());

                  deferredOtherVariantAttrs.add(
                          AttributeBuilder.of()
                                  .name(a.getName())
                                  .value(entry)
                                  .build());
                  //deferredOtherVariantAttrs.add(attr);

              } else {
                  plainAttrs.add(a);
              }
          }

          cleanedVariants.add(
                  ProductVariantDraftBuilder.of(v)
                          .attributes(plainAttrs)
                          .build()
          );
      }

      // 2) create draft WITHOUT productRef attributes
      final ProductDraft createDraft =
              ProductDraftBuilder.of(productDraft)
                      .attributes(plainProductLevelAttrs)
                      .masterVariant(
                              productDraft.getMasterVariant() == null
                                      ? null
                                      : ProductVariantDraftBuilder.of(productDraft.getMasterVariant())
                                      .attributes(plainMasterVariantAttrs)   // IMPORTANT
                                      .build()
                      )
                      .variants(cleanedVariants)
                      .build();


      // 3) persist deferred attrs (product-level + master-variant-level), then create product
      final CompletionStage<Void> saveProductAttrsStage =
              deferredProductLevelAttrs.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : saveDeferredAttributesToCustomObject(productDraft.getKey(), deferredProductLevelAttrs);

      final CompletionStage<Void> saveVariantAttrsStage =
              deferredMasterVariantAttrs.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : saveDeferredVariantAttributesToCustomObject(DEFERRED_MASTER_VARIANT_ATTRS_CONTAINER , productDraft.getKey(), deferredMasterVariantAttrs);

      final CompletionStage<Void> saveOtherVariantsStage =
              deferredOtherVariantAttrs.isEmpty()
                      ? CompletableFuture.completedFuture(null)
                      : saveDeferredVariantAttributesToCustomObject(DEFERRED_VARIANT_ATTRS_CONTAINER, productDraft.getKey(), deferredOtherVariantAttrs);


      return CompletableFuture.allOf(
                      saveProductAttrsStage.toCompletableFuture(),
                      saveVariantAttrsStage.toCompletableFuture(),
                      saveOtherVariantsStage.toCompletableFuture())
              .thenCompose(ignored ->
                              getSyncOptions()
                                      .applyBeforeCreateCallback(createDraft)
                                      .map(
                                              draft ->
                                                      productService
                                                              .createProduct(draft)
                                                              .thenAccept(
                                                                      productOptional -> {
                                                                          if (productOptional.isPresent()) {
                                                                              readyToResolve.add(productDraft.getKey());
                                                                              statistics.incrementCreated();
                                                                          } else {
                                                                              statistics.incrementFailed();
                                                                          }
                                                                      }))
                                      .orElse(CompletableFuture.completedFuture(null)));
  }

    /**
   * Given a {@link String} {@code errorMessage} and a {@link Throwable} {@code exception}, this
   * method calls the optional error callback specified in the {@code syncOptions} and updates the
   * {@code statistics} instance by incrementing the total number of failed products to sync. <br>
   * <br>
   * NOTE: This method is similar to {@link BaseSync#handleError}. It is left here because of usage
   * of different classes for view ({@link ProductProjection}) and updates ({@link Product}) from
   * commercetools. This is specific only for ProductSync.
   *
   * @param errorMessage The error message describing the reason(s) of failure.
   * @param exception The exception that called caused the failure, if any.
   * @param oldProduct the ProductProjection which could be updated.
   * @param newProduct the ProductProjection draft where we get the new data.
   * @param updateActions the update actions to update the {@link Product} with.
   */
  private void handleProductSyncError(
      @Nonnull final String errorMessage,
      @Nullable final Throwable exception,
      @Nullable final ProductProjection oldProduct,
      @Nullable final ProductDraft newProduct,
      @Nullable final List<ProductUpdateAction> updateActions) {
    SyncException syncException =
        exception != null
            ? new SyncException(errorMessage, exception)
            : new SyncException(errorMessage);
    syncOptions.applyErrorCallback(syncException, oldProduct, newProduct, updateActions);
    statistics.incrementFailed();
  }

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();


    private static boolean isProductReferenceAttribute(@Nullable final Attribute attr) {
        if (attr == null) return false;

        final Object value = attr.getValue();
        if (value == null) return false;

        // Handle true SDK Reference instances
        if (value instanceof com.commercetools.api.models.common.Reference) {
            final com.commercetools.api.models.common.Reference ref =
                    (com.commercetools.api.models.common.Reference) value;

            // DON'T use enum comparison here (it caused you “always false” warnings earlier).
            final Object typeId = ref.getTypeId();
            return typeId != null && "product".equalsIgnoreCase(String.valueOf(typeId));
        }

        // Handle values that are NOT Reference objects (often Map/JsonNode-like),
        // but represent a product reference (typeId=product + (id|key))
        try {
            final JsonNode node = OBJECT_MAPPER.valueToTree(value);
            if (node == null || !node.isObject()) return false;

            final JsonNode typeIdNode = node.get("typeId");
            if (typeIdNode == null) return false;

            final String typeId = typeIdNode.asText();
            if (!"product".equalsIgnoreCase(typeId)) return false;

            // If it has either "id" or "key", it's a product reference-ish payload.
            return node.hasNonNull("id") || node.hasNonNull("key");
        } catch (Exception e) {
            return false;
        }
    }


    private static final String DEFERRED_ATTR_CONTAINER = "deferred-product-attrs";

    @Nonnull
    private CompletionStage<Void> saveDeferredAttributesToCustomObject(
            @Nonnull final String productKey,
            @Nonnull final List<Attribute> deferredAttrs) {

        final CustomObjectDraft draft =
                CustomObjectDraftBuilder.of()
                        .container(DEFERRED_ATTR_CONTAINER)
                        .key(productKey)
                        .value(toDeferredEntries(deferredAttrs))
                        .build();

        return getSyncOptions()
                .getCtpClient()
                .customObjects()
                .post(draft)
                .execute()
                .thenApply(resp -> null);
    }

    @Nonnull
    private CompletionStage<Void> saveDeferredVariantAttributesToCustomObject(
            @Nonnull final String containerName,
            @Nonnull final String productKey,
            @Nonnull final List<Attribute> deferredVariantAttrs) {

        final CustomObjectDraft draft =
                CustomObjectDraftBuilder.of()
                        .container(containerName)
                        .key(productKey)
                        .value(toDeferredEntries(deferredVariantAttrs))
                        .build();

        return getSyncOptions()
                .getCtpClient()
                .customObjects()
                .post(draft)
                .execute()
                .thenApply(r -> null);
    }


    static final class DeferredAttributesPayload {
        public List<Attribute> productLevel;
        public List<Attribute> masterVariant;

        DeferredAttributesPayload(
                List<Attribute> productLevel,
                List<Attribute> masterVariant) {
            this.productLevel = productLevel;
            this.masterVariant = masterVariant;
        }
    }

    // Put near your other helpers / constants
    static final class DeferredAttrEntry {
        public String name;
        public Object value;

        DeferredAttrEntry() {}

        DeferredAttrEntry(String name, Object value) {
            this.name = name;
            this.value = value;
        }
    }

    @Nonnull
    private static List<DeferredAttrEntry> toDeferredEntries(@Nonnull final List<Attribute> attrs) {
        final List<DeferredAttrEntry> out = new ArrayList<>(attrs.size());
        for (Attribute a : attrs) {
            if (a != null) out.add(new DeferredAttrEntry(a.getName(), a.getValue()));
        }
        return out;
    }

    // Add near other constants
    private static final ObjectMapper DEFERRED_ATTRS_MAPPER = new ObjectMapper();

    private CompletionStage<Void> applyDeferredAttributes(
            @Nonnull final String productKey,
            @Nonnull final com.commercetools.api.models.product.Product product,
            @Nullable final DeferredAttributesPayload payload,
            @Nonnull final Map<String, String> productKeyToIdCache
    ) {
        // We will still apply "other variants" even if payload is null,
        // because those are stored in a separate custom object container.
        final DeferredAttributesPayload safePayload = payload;

        // 1) Fetch deferred OTHER VARIANT attrs from custom object container
        final CompletionStage<List<DeferredAttrEntry>> deferredVariantEntriesStage =
                getSyncOptions().getCtpClient()
                        .customObjects()
                        .withContainerAndKey(DEFERRED_VARIANT_ATTRS_CONTAINER, productKey)
                        .get()
                        .execute()
                        .thenApply(apiResponse -> {
                            final com.commercetools.api.models.custom_object.CustomObject co =
                                    apiResponse != null ? apiResponse.getBody() : null;
                            if (co == null || co.getValue() == null) {
                                return java.util.Collections.<DeferredAttrEntry>emptyList();
                            }
                            try {
                                return DEFERRED_ATTRS_MAPPER.convertValue(
                                        co.getValue(),
                                        new com.fasterxml.jackson.core.type.TypeReference<List<DeferredAttrEntry>>() {}
                                );
                            } catch (Exception e) {
                                // If parsing fails, do not break sync
                                return java.util.Collections.<DeferredAttrEntry>emptyList();
                            }
                        })
                        .exceptionally(ex -> java.util.Collections.emptyList());

        return deferredVariantEntriesStage.thenCompose(deferredVariantEntries -> {

            final List<com.commercetools.api.models.product.ProductUpdateAction> actions = new ArrayList<>();

            // -------------------------
            // Product-level attributes (must use ProductSetProductAttributeAction)
            // -------------------------
            if (safePayload != null && safePayload.productLevel != null && !safePayload.productLevel.isEmpty()) {
                for (com.commercetools.api.models.product.Attribute a : safePayload.productLevel) {
                    final Object normalized = normalizeDeferredAttributeValue(a.getValue(), productKeyToIdCache);

                    actions.add(
                            com.commercetools.api.models.product.ProductSetProductAttributeActionBuilder.of()
                                    .name(a.getName())
                                    .value(normalized)
                                    .staged(false)
                                    .build()
                    );
                }
            }

            // -------------------------
            // Master-variant attributes (variantId required)
            // -------------------------
            final Long masterVariantId =
                    product.getMasterData() != null
                            && product.getMasterData().getStaged() != null
                            && product.getMasterData().getStaged().getMasterVariant() != null
                            ? product.getMasterData().getStaged().getMasterVariant().getId()
                            : null;

            if (masterVariantId != null
                    && safePayload != null
                    && safePayload.masterVariant != null
                    && !safePayload.masterVariant.isEmpty()) {

                for (com.commercetools.api.models.product.Attribute a : safePayload.masterVariant) {
                    final Object normalized = normalizeDeferredAttributeValue(a.getValue(), productKeyToIdCache);

                    actions.add(
                            com.commercetools.api.models.product.ProductSetAttributeActionBuilder.of()
                                    .variantId(masterVariantId)
                                    .name(a.getName())
                                    .value(normalized)
                                    .staged(false)
                                    .build()
                    );
                }
            }

            // -------------------------
            // OTHER VARIANTS (NEW)
            // Expecting DeferredAttrEntry.value in ONE of these shapes:
            //   A) { "sku": "<variantSku>", "value": <realValue> }  -> apply to that variant using sku()
            //   B) <realValue>                                     -> if no sku wrapper, we apply to master (if masterVariantId exists)
            // -------------------------
            if (deferredVariantEntries != null && !deferredVariantEntries.isEmpty()) {
                for (DeferredAttrEntry e : deferredVariantEntries) {
                    if (e == null || e.name == null) {
                        continue;
                    }

                    final Object raw = e.value;

                    // Case A: wrapper map containing sku + value
                    if (raw instanceof Map) {
                        @SuppressWarnings("unchecked")
                        final Map<String, Object> m = (Map<String, Object>) raw;

                        final Object skuObj = m.get("sku");
                        final Object valObj = m.get("value");

                        if (skuObj instanceof String && !((String) skuObj).isBlank()) {
                            final String variantSku = (String) skuObj;
                            final Object normalized = normalizeDeferredAttributeValue(valObj, productKeyToIdCache);

                            actions.add(
                                    com.commercetools.api.models.product.ProductSetAttributeActionBuilder.of()
                                            .sku(variantSku)              // ✅ identifies the variant
                                            .name(e.name)
                                            .value(normalized)
                                            .staged(false)
                                            .build()
                            );
                            continue;
                        }
                    }

                    // Case B: no sku wrapper available -> apply to master (safe fallback)
                    if (masterVariantId != null) {
                        final Object normalized = normalizeDeferredAttributeValue(raw, productKeyToIdCache);

                        actions.add(
                                com.commercetools.api.models.product.ProductSetAttributeActionBuilder.of()
                                        .variantId(masterVariantId)
                                        .name(e.name)
                                        .value(normalized)
                                        .staged(false)
                                        .build()
                        );
                    }
                }
            }

            if (actions.isEmpty()) {
                return CompletableFuture.completedFuture(null);
            }

            // Fetch projection then update, then delete BOTH custom objects (best-effort)
            return productService.fetchProduct(productKey)
                    .thenCompose(optionalProjection -> {
                        if (optionalProjection == null || optionalProjection.isEmpty()) {
                            return CompletableFuture.completedFuture(null);
                        }

                        final com.commercetools.api.models.product.ProductProjection projection = optionalProjection.get();

                        return productService.updateProduct(projection, actions)
                                .thenCompose(ignore ->
                                        // delete deferred-product-attributes
                                        getSyncOptions().getCtpClient()
                                                .customObjects()
                                                .withContainerAndKey(CUSTOM_OBJECT_DEFERRED_ATTRS, productKey)
                                                .delete()
                                                .execute()
                                                .handle((r, ex) -> null)
                                                // delete deferred-variant-attrs
                                                .thenCompose(x ->
                                                        getSyncOptions().getCtpClient()
                                                                .customObjects()
                                                                .withContainerAndKey(DEFERRED_VARIANT_ATTRS_CONTAINER, productKey)
                                                                .delete()
                                                                .execute()
                                                                .handle((r2, ex2) -> null)
                                                )
                                                .thenApply(r3 -> (Void) null)
                                );
                    });
        });
    }

    private CompletionStage<Void> applyDeferredOtherVariantAttributes(
            @Nonnull final String productKey,
            @Nonnull final Product product,
            @Nonnull final Map<String, String> productKeyToIdCache
    ) {
        return getSyncOptions().getCtpClient()
                .customObjects()
                .withContainerAndKey(DEFERRED_VARIANT_ATTRS_CONTAINER, productKey)
                .get()
                .execute()
                .thenCompose(resp -> {

                    if (resp == null || resp.getBody() == null) {
                        return CompletableFuture.completedFuture(null);
                    }

                    @SuppressWarnings("unchecked")
                    List<Map<String, Object>> entries =
                            (List<Map<String, Object>>) resp.getBody().getValue();

                    if (entries == null || entries.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    final List<ProductUpdateAction> actions = new ArrayList<>();

                    for (Map<String, Object> e : entries) {
                        final String name = (String) e.get("name");

                        @SuppressWarnings("unchecked")
                        Map<String, Object> wrapped = (Map<String, Object>) e.get("value");

                        if (wrapped == null) continue;

                        final String sku = (String) wrapped.get("sku");
                        final Object rawValue = wrapped.get("value");

                        if (sku == null || rawValue == null) continue;

                        final Object normalized =
                                normalizeDeferredAttributeValue(rawValue, productKeyToIdCache);

                        actions.add(
                                ProductSetAttributeActionBuilder.of()
                                        .sku(sku)
                                        .name(name)
                                        .value(normalized)
                                        .staged(false)
                                        .build()
                        );
                    }

                    if (actions.isEmpty()) {
                        return CompletableFuture.completedFuture(null);
                    }

                    return productService.fetchProduct(productKey)
                            .thenCompose(opt ->
                                    opt.map(p ->
                                            productService.updateProduct(p, actions)
                                    ).orElse(CompletableFuture.completedFuture(null))
                            )
                            .thenCompose(v ->
                                    getSyncOptions().getCtpClient()
                                            .customObjects()
                                            .withContainerAndKey(
                                                    DEFERRED_VARIANT_ATTRS_CONTAINER, productKey)
                                            .delete()
                                            .execute()
                                            .handle((r, ex) -> null)
                            );
                });
    }



    private CompletionStage<Void> applyDeferredAttributesForSyncedProducts(
            @Nonnull final Set<ProductDraft> syncedDrafts,
            @Nonnull final Map<String, String> productKeyToIdCache) {

        if (syncedDrafts.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }

        final ProjectApiRoot client = getSyncOptions().getCtpClient();

        final CompletableFuture<?>[] futures = syncedDrafts.stream()
                .map(ProductDraft::getKey)
                .filter(Objects::nonNull)
                .distinct()
                .map(productKey ->
                        client.products()
                                .withKey(productKey)
                                .get()
                                .execute()
                                .thenCompose(productResponse -> {

                                    final Product product = productResponse.getBody();

                                    return fetchDeferredAttributesPayload(productKey)
                                            .thenCompose(payload ->
                                                    applyDeferredAttributes(
                                                            productKey,
                                                            product,
                                                            payload,
                                                            productKeyToIdCache
                                                    ).toCompletableFuture()
                                            )
                                            .thenCompose(v ->
                                            applyDeferredOtherVariantAttributes(
                                                    productKey,
                                                    product,
                                                    productKeyToIdCache
                                            )
                                    );
                                })
                                // Defensive: do not break batch on failure
                                .exceptionally(ex -> null)
                )
                .toArray(CompletableFuture[]::new);

        return CompletableFuture.allOf(futures).thenApply(v -> null);
    }



    private CompletionStage<DeferredAttributesPayload> fetchDeferredAttributesPayload(
            @Nonnull final String productKey) {

        final ProjectApiRoot client = getSyncOptions().getCtpClient();

        final CompletableFuture<List<DeferredAttrEntry>> productEntriesFuture =
                fetchDeferredEntries(client, DEFERRED_ATTR_CONTAINER, productKey);

        /*final CompletableFuture<List<DeferredAttrEntry>> variantEntriesFuture =
                fetchDeferredEntries(client, DEFERRED_VARIANT_ATTRS_CONTAINER, productKey);*/

        final CompletableFuture<List<DeferredAttrEntry>> masterVariantEntriesFuture =
                fetchDeferredEntries(client, DEFERRED_MASTER_VARIANT_ATTRS_CONTAINER, productKey);

        return productEntriesFuture.thenCombine(masterVariantEntriesFuture, (productEntries, variantEntries) -> {

            if ((productEntries == null || productEntries.isEmpty())
                    && (variantEntries == null || variantEntries.isEmpty())) {
                return null;
            }
            final List<Attribute> productAttrs = toAttributes(productEntries);
            final List<Attribute> masterVariantAttrs = toAttributes(variantEntries);

            return new DeferredAttributesPayload(productAttrs, masterVariantAttrs);
        });
    }




    /**
     * Converts stored DeferredAttrEntry list into Attribute list expected by DeferredAttributesPayload.
     */
    @Nonnull
    private static List<com.commercetools.api.models.product.Attribute> toAttributes(
            @Nonnull final List<DeferredAttrEntry> entries) {

        if (entries.isEmpty()) {
            return Collections.emptyList();
        }

        final List<com.commercetools.api.models.product.Attribute> out = new ArrayList<>(entries.size());
        for (DeferredAttrEntry e : entries) {
            if (e == null || e.name == null) {
                continue;
            }
            out.add(
                    com.commercetools.api.models.product.AttributeBuilder.of()
                            .name(e.name)
                            .value(e.value)
                            .build()
            );
        }
        return out;
    }

    private static CompletableFuture<List<DeferredAttrEntry>> fetchDeferredEntries(
            @Nonnull final ProjectApiRoot client,
            @Nonnull final String container,
            @Nonnull final String key) {

        return client.customObjects()
                .withContainerAndKey(container, key)
                .get()
                .execute()
                .thenApply(resp -> {
                    final CustomObject co = resp.getBody();
                    if (co == null || co.getValue() == null) {
                        return Collections.<DeferredAttrEntry>emptyList();
                    }

                    // Value is stored as JSON (you store List<DeferredAttrEntry> via toDeferredEntries)
                    // Convert defensively.
                    try {
                        return DEFERRED_ATTRS_MAPPER.convertValue(
                                co.getValue(),
                                new com.fasterxml.jackson.core.type.TypeReference<List<DeferredAttrEntry>>() {});
                    } catch (Exception ex) {
                        return Collections.<DeferredAttrEntry>emptyList();
                    }
                })
                .exceptionally(ex -> {
                    // 404 (not found) or any other issue -> treat as "no deferred attrs"
                    return Collections.emptyList();
                });
    }

    private static CompletableFuture<Void> deleteCustomObjectIfExists(
            @Nonnull final ProjectApiRoot client,
            @Nonnull final String container,
            @Nonnull final String key) {

        return client.customObjects()
                .withContainerAndKey(container, key)
                .get()
                .execute()
                .thenCompose(getResp -> {
                    final CustomObject co = getResp.getBody();
                    if (co == null) return CompletableFuture.completedFuture(null);

                    return client.customObjects()
                            .withContainerAndKey(container, key)
                            .delete()
                            .withVersion(co.getVersion())
                            .execute()
                            .thenApply(delResp -> null);
                })
                .thenApply(x -> null);
    }

    private Object normalizeDeferredAttributeValue(
            @Nullable final Object value,
            @Nonnull final Map<String, String> productKeyToIdCache
    ) {
        if (value == null) {
            return null;
        }

        // If list / set: normalize each element
        if (value instanceof List) {
            final List<?> in = (List<?>) value;
            final List<Object> out = new ArrayList<>(in.size());
            for (Object v : in) {
                out.add(normalizeDeferredAttributeValue(v, productKeyToIdCache));
            }
            return out;
        }

        // If map: normalize nested values AND also handle map-shaped references
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            final Map<String, Object> map = new LinkedHashMap<>((Map<String, Object>) value);

            final Object typeIdObj = map.get("typeId");
            final String typeId = typeIdObj != null ? String.valueOf(typeIdObj) : null;

            if ("product".equalsIgnoreCase(typeId)) {
                // If it's {typeId:"product", id:"..."} -> convert directly
                final Object idObj = map.get("id");
                if (idObj != null) {
                    final String id = String.valueOf(idObj);
                    return ProductResourceIdentifierBuilder.of().id(id).build();
                }

                // If it's {typeId:"product", key:"CS-...."} -> resolve key->id then convert
                final Object keyObj = map.get("key");
                if (keyObj != null) {
                    final String key = String.valueOf(keyObj);
                    final String resolvedId = productKeyToIdCache.get(key);
                    if (resolvedId != null && !resolvedId.isBlank()) {
                        return ProductResourceIdentifierBuilder.of().id(resolvedId).build();
                    }
                }

                // still unresolved -> leave as-is (will retry next run)
                return value;
            }

            // Not a product reference; still normalize nested map fields
            for (Map.Entry<String, Object> e : map.entrySet()) {
                map.put(e.getKey(), normalizeDeferredAttributeValue(e.getValue(), productKeyToIdCache));
            }
            return map;
        }

        // If it is a commercetools "Reference" object
        if (value instanceof com.commercetools.api.models.common.Reference) {
            final com.commercetools.api.models.common.Reference ref =
                    (com.commercetools.api.models.common.Reference) value;

            final Object typeIdObj = ref.getTypeId();
            final String typeId = typeIdObj != null ? String.valueOf(typeIdObj) : null;

            if (!"product".equalsIgnoreCase(typeId)) {
                return value;
            }

            // typed Reference usually has id
            final String id = ref.getId();
            if (id != null && !id.isBlank()) {
                return ProductResourceIdentifierBuilder.of().id(id).build();
            }

            // If only key is present somewhere, we can't read it from this Reference type reliably.
            return value;
        }

        // Anything else: leave unchanged
        return value;
    }

    static final class DeferredVariantAttribute {
        String variantKey;          // REQUIRED
        Attribute attribute;        // your existing Attribute

        DeferredVariantAttribute(String variantKey, Attribute attribute) {
            this.variantKey = variantKey;
            this.attribute = attribute;
        }
    }


}
