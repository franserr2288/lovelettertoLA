from dataclasses import dataclass
from datetime import date
from typing import Optional

# keys are capitalized differently in their api, using this more for documentation
# not sure if i want another change to ingestion, to transform into a common model
@dataclass
class ServiceRequest:
    sr_number: str
    created_date: str
    request_type: str
    status: str

    anonymous: str # N = no, Y = yes
    address_verified: str
    approximate_address: str
    
    # timestamps
    updated_date: Optional[str] = None
    service_date: Optional[date] = None
    closed_date: Optional[str] = None
    
    # request meta data
    action_taken: Optional[str] = None
    owner: Optional[str] = None  # dept or person responsible
    assign_to: Optional[str] = None
    request_source: Optional[str] = None  # phone, web, mobile app, etc.
    created_by_user_organization: Optional[str] = None
    mobile_os: Optional[str] = None
    
    # address info
    address: Optional[str] = None
    house_number: Optional[str] = None
    direction: Optional[str] = None  # N, S, E, W
    street_name: Optional[str] = None
    suffix: Optional[str] = None  # St, Ave, Blvd
    zipcode: Optional[str] = None
    
    # geolocation
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    location: Optional[dict] = None
    
    # thomas brothers map references AKA grid-wise location
    tbm_page: Optional[int] = None
    tbm_column: Optional[str] = None
    tbm_row: Optional[str] = None
    
    # bureaucratic info 
    area_planning_commission_district: Optional[str] = None
    council_district_number: Optional[str] = None
    council_district_member_name: Optional[str] = None
    neighborhood_council_identifier: Optional[str] = None
    neighborhood_council_name: Optional[str] = None
    police_precinct: Optional[str] = None