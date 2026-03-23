-- Gold layer: SCD2 fact table – public interface over the dbt snapshot.
-- Renames dbt-generated columns, adds neighbourhood FK, calculated metrics,
-- and is_current for easy filtering.

SELECT
    dbt_scd_id        AS scd_id,
    -- Core identification
    listing_id,
    website,
    url,
    published_at,
    title,
    -- Address (street is a degenerate dimension — lives in the fact)
    street,
    -- FK to dim_neighbourhoods (replaces city + neighbourhood)
    md5(neighbourhood || '~' || city) AS neighbourhood_sk,
    -- Pricing & dates
    rent,
    move_in_date,
    -- Calculated metrics
    rent / NULLIF(size_sqft, 0) AS price_per_sqft,
    -- Property details
    bedrooms,
    bathrooms,
    size_sqft,
    unit_type,
    agreement_type,
    furnished,
    for_rent_by,
    -- Location
    latitude,
    longitude,
    -- Amenities: Infrastructure
    elevator,
    gym,
    concierge,
    security_24h,
    pool,
    -- Amenities: Living features
    balcony,
    yard,
    storage_space,
    -- Amenities: Utilities
    heat,
    water,
    hydro,
    internet,
    cable_tv,
    -- Amenities: Laundry & Parking
    laundry_in_unit,
    laundry_in_building,
    parking_included,
    -- Amenities: Kitchen
    dishwasher,
    fridge_freezer,
    -- Amenities: Pet & Accessibility
    pet_friendly,
    smoking_permitted,
    wheelchair_accessible,
    barrier_free,
    accessible_washrooms,
    audio_prompts,
    visual_aids,
    braille_labels,
    -- Amenities: Other
    bicycle_parking,
    air_conditioning,
    -- Change detection
    row_hash,
    -- Temporal (source)
    scraped_at,
    -- SCD2 temporal
    dbt_valid_from    AS valid_from,
    dbt_valid_to      AS valid_to,
    dbt_valid_to IS NULL AS is_current,
    dbt_updated_at    AS updated_at
FROM {{ ref('_snap_fct_rental_listings') }}
