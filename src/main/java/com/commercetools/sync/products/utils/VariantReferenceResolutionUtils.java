package com.commercetools.sync.products.utils;

import static com.commercetools.sync.commons.utils.AssetReferenceResolutionUtils.mapToAssetDrafts;
import static com.commercetools.sync.commons.utils.ResourceIdentifierUtils.REFERENCE_ID_FIELD;
import static java.util.stream.Collectors.toList;

import com.commercetools.api.models.common.PriceDraft;
import com.commercetools.api.models.product.*;
import com.commercetools.sync.commons.utils.ReferenceIdToKeyCache;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Util class which provides utilities that can be used when syncing resources from a source
 * commercetools project to a target one.
 */
public final class VariantReferenceResolutionUtils {

  /**
   * Returns an {@link List}&lt;{@link ProductVariantDraft}&gt; consisting of the results of
   * applying the mapping from {@link ProductVariant} to {@link ProductVariantDraft} with
   * considering reference resolution.
   *
   * <p><b>Note:</b> The aforementioned references should be cached(idToKey value fetched and stored
   * in a map). Any reference that is not cached will have its id in place and not replaced by the
   * key will be considered as existing resources on the target commercetools project and the
   * library will issues an update/create API request without reference resolution.
   *
   * @param productVariants the product variants without expansion of references.
   * @param referenceIdToKeyCache the instance that manages cache.
   * @return a {@link List} of {@link ProductVariantDraft} built from the supplied {@link List} of
   *     {@link ProductVariant}.
   */
  @Nonnull
  public static List<ProductVariantDraft> mapToProductVariantDrafts(
      @Nonnull final List<ProductVariant> productVariants,
      @Nonnull final ReferenceIdToKeyCache referenceIdToKeyCache) {

    return productVariants.stream()
        .filter(Objects::nonNull)
        .map(variants -> mapToProductVariantDraft(variants, referenceIdToKeyCache))
        .collect(toList());
  }

  @Nonnull
  private static ProductVariantDraft mapToProductVariantDraft(
      @Nonnull final ProductVariant productVariant,
      @Nonnull final ReferenceIdToKeyCache referenceIdToKeyCache) {

    // ✅ FIX: ensure any reference IDs inside variant attributes are replaced with keys
    final List<Attribute> resolvedAttributes =
            replaceAttributeReferenceIdsWithKeys(productVariant.getAttributes(), referenceIdToKeyCache);

    return ProductVariantDraftBuilder.of()
        .sku(productVariant.getSku())
        .key(productVariant.getKey())
        .prices(mapToPriceDrafts(productVariant, referenceIdToKeyCache))
        .attributes(resolvedAttributes)
        .assets(mapToAssetDrafts(productVariant.getAssets(), referenceIdToKeyCache))
        .images(productVariant.getImages())
        .build();
  }

  @Nonnull
  static List<PriceDraft> mapToPriceDrafts(
      @Nonnull final ProductVariant productVariant,
      @Nonnull final ReferenceIdToKeyCache referenceIdToKeyCache) {

    return PriceUtils.createPriceDraft(productVariant.getPrices(), referenceIdToKeyCache);
  }

  /**
   * Defensive: replaces reference "id" fields inside attribute values with keys from cache.
   * This prevents create failures like: "is not valid for field 'productRef'".
   *
   * NOTE: This mutates attribute values by converting them into JsonNodes via AttributeUtils,
   * which is how sync-java already handles attribute reference replacement elsewhere.
   */
  @Nonnull
  private static List<Attribute> replaceAttributeReferenceIdsWithKeys(
          @Nullable final List<Attribute> attributes,
          @Nonnull final ReferenceIdToKeyCache referenceIdToKeyCache) {

    if (attributes == null || attributes.isEmpty()) {
      return attributes;
    }

    // copy list defensively (in case the original list is immutable)
    final List<Attribute> attrs = new ArrayList<>(attributes);

    for (Attribute attr : attrs) {
      if (attr == null) {
        continue;
      }

      // Convert value to JsonNode (mutates the attribute's value internally)
      final JsonNode valueNode = AttributeUtils.replaceAttributeValueWithJsonAndReturnValue(attr);

      // Collect all references found in that value
      final List<JsonNode> refs = AttributeUtils.getAttributeReferences(valueNode);
      if (refs == null || refs.isEmpty()) {
        continue;
      }

      // Replace "id" with cached key (if present)
      for (JsonNode ref : refs) {
        if (ref == null || !ref.hasNonNull(REFERENCE_ID_FIELD)) {
          continue;
        }

        final String id = ref.get(REFERENCE_ID_FIELD).asText();
        final String key = referenceIdToKeyCache.get(id);

        if (key != null && ref instanceof ObjectNode) {
          final ObjectNode refObj = (ObjectNode) ref;
          refObj.remove(REFERENCE_ID_FIELD);
          refObj.put("key", key);
        }
      }
    }

    return attrs;
  }

  private VariantReferenceResolutionUtils() {}
}
