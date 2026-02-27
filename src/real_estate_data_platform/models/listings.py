"""Data models for real estate listings."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field, field_validator

from real_estate_data_platform.models.enums import City


class RentalsListing(BaseModel):
    """Model for a single Rentals listing."""

    # Core identification
    listing_id: str = Field(description="Native listing ID from the source site")
    url: str
    website: str = Field(description="Source website (e.g., 'kijiji')")
    published_at: datetime = Field(description="Original publish date from the listing")
    title: str
    description: str

    # Address information
    street: str
    city: City
    neighbourhood: str | None = None

    # Pricing and dates
    rent: float | None = Field(None, ge=0)
    move_in_date: str | None = None

    # Property details
    bedrooms: int | None = Field(None, ge=0)
    bathrooms: int | None = Field(None, ge=0)
    size_sqft: float | None = Field(None, ge=0)
    unit_type: str | None = None
    agreement_type: str | None = None
    furnished: str | None = None
    for_rent_by: str | None = None

    # Location details
    latitude: float | None = None
    longitude: float | None = None
    walk_score: float | None = Field(None, ge=0, le=10)
    transit_score: float | None = Field(None, ge=0, le=10)
    bike_score: float | None = Field(None, ge=0, le=10)

    # Media
    images: list[str] = Field(default_factory=list)

    # Amenities - Infrastructure
    elevator: str | None = None
    gym: str | None = None
    concierge: str | None = None
    security_24h: str | None = None
    pool: str | None = None

    # Amenities - Living Features
    balcony: str | None = None
    yard: str | None = None
    storage_space: str | None = None

    # Amenities - Utilities
    heat: str | None = None
    water: str | None = None
    hydro: str | None = None
    internet: str | None = None
    cable_tv: str | None = None

    # Amenities - Laundry & Parking
    laundry_in_unit: str | None = None
    laundry_in_building: str | None = None
    parking_included: str | None = None

    # Amenities - Kitchen
    dishwasher: str | None = None
    fridge_freezer: str | None = None

    # Amenities - Pet & Accessibility
    pet_friendly: str | None = None
    smoking_permitted: str | None = None
    wheelchair_accessible: str | None = None
    barrier_free: str | None = None
    accessible_washrooms: str | None = None

    # Amenities - Accessibility Features
    audio_prompts: str | None = None
    visual_aids: str | None = None
    braille_labels: str | None = None

    # Amenities - Other
    bicycle_parking: str | None = None
    air_conditioning: str | None = None

    # Metadata
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        """Validate URL format."""
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v

    @field_validator("published_at")
    @classmethod
    def validate_published_date(cls, v: datetime) -> datetime:
        """Validate published_at is not in the future."""
        if v > datetime.now(UTC):
            raise ValueError("published_at cannot be in the future")
        return v
