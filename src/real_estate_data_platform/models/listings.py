"""Data models for real estate listings."""

from datetime import UTC, datetime

from pydantic import BaseModel, Field

from real_estate_data_platform.models.enums import City


class RentalsListing(BaseModel):
    """Model for a single Rentals listing."""

    # Core identification
    listing_id: str = Field(description="Native listing ID from the source site")
    url: str = Field(description="Listing URL")
    website: str = Field(description="Source website (e.g., 'kijiji')")
    published_at: datetime | None = Field(
        None, description="Original publish date from the listing"
    )
    title: str = Field(description="Listing title")
    description: str = Field(description="Listing description")

    # Address information
    street: str = Field(description="Street address")
    city: City = Field(..., description="City name from supported list")
    neighbourhood: str | None = Field(None, description="Neighbourhood name")

    # Pricing and dates
    rent: float | None = Field(None, description="Monthly rent in CAD", ge=0)
    move_in_date: str | None = Field(None, description="Move-in date")

    # Property details
    bedrooms: int | None = Field(None, description="Number of bedrooms", ge=0)
    bathrooms: int | None = Field(None, description="Number of bathrooms", ge=0)
    size_sqft: float | None = Field(None, description="Property size in square feet", ge=0)
    unit_type: str | None = Field(None, description="Unit type (condo, apartment, house, etc.)")
    agreement_type: str | None = Field(None, description="Agreement type (lease, sublet, etc.)")
    furnished: str | None = Field(None, description="Is furnished")
    for_rent_by: str | None = Field(None, description="Listed by (landlord, agent, etc.)")

    # Location details
    latitude: float | None = Field(None, description="Latitude coordinate")
    longitude: float | None = Field(None, description="Longitude coordinate")
    walk_score: float | None = Field(None, description="Walk Score", ge=0, le=10)
    transit_score: float | None = Field(None, description="Transit Score", ge=0, le=10)
    bike_score: float | None = Field(None, description="Bike Score", ge=0, le=10)

    # Media
    images: list[str] = Field(default_factory=list, description="List of image URLs")

    # Amenities - Infrastructure
    elevator: str | None = Field(None, description="Elevator in building")
    gym: str | None = Field(None, description="Gym available")
    concierge: str | None = Field(None, description="Concierge service")
    security_24h: str | None = Field(None, description="24-hour security")
    pool: str | None = Field(None, description="Pool available")

    # Amenities - Living Features
    balcony: str | None = Field(None, description="Balcony")
    yard: str | None = Field(None, description="Yard")
    storage_space: str | None = Field(None, description="Storage space")

    # Amenities - Utilities
    heat: str | None = Field(None, description="Heat included")
    water: str | None = Field(None, description="Water included")
    hydro: str | None = Field(None, description="Hydro (electricity) included")
    internet: str | None = Field(None, description="Internet included")
    cable_tv: str | None = Field(None, description="Cable/TV included")

    # Amenities - Laundry & Parking
    laundry_in_unit: str | None = Field(None, description="Laundry in unit")
    laundry_in_building: str | None = Field(None, description="Laundry in building")
    parking_included: str | None = Field(None, description="Parking included")

    # Amenities - Kitchen
    dishwasher: str | None = Field(None, description="Dishwasher")
    fridge_freezer: str | None = Field(None, description="Fridge/Freezer")

    # Amenities - Pet & Accessibility
    pet_friendly: str | None = Field(None, description="Pet friendly")
    smoking_permitted: str | None = Field(None, description="Smoking permitted")
    wheelchair_accessible: str | None = Field(None, description="Wheelchair accessible")
    barrier_free: str | None = Field(None, description="Barrier-free entrances and ramps")
    accessible_washrooms: str | None = Field(None, description="Accessible washrooms in suite")

    # Amenities - Accessibility Features
    audio_prompts: str | None = Field(None, description="Audio prompts")
    visual_aids: str | None = Field(None, description="Visual aids")
    braille_labels: str | None = Field(None, description="Braille labels")

    # Amenities - Other
    bicycle_parking: str | None = Field(None, description="Bicycle parking")
    air_conditioning: str | None = Field(None, description="Air conditioning")

    # Metadata
    scraped_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
