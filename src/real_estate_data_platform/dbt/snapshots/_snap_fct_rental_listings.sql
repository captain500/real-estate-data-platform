{% snapshot _snap_fct_rental_listings %}

{{
    config(
        target_schema='gold',
        unique_key="listing_id || '~' || website",
        strategy='check',
        check_cols=['row_hash'],
    )
}}

SELECT
    -- Core identification
    listing_id,
    website,
    url,
    published_at,
    title,
    -- Address
    street,
    city,
    neighbourhood,
    -- Pricing & dates
    rent,
    move_in_date,
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
    -- Temporal
    scraped_at
-- TODO: cuando silver crezca (100k+ filas), considerar añadir filtro temporal
-- para evitar escanear toda la tabla. Ejemplo:
-- WHERE scraped_at >= current_date - interval '7 days'
FROM {{ source('silver', 'rental_listings') }}

{% endsnapshot %}
