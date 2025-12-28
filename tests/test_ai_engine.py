
import pytest
from datetime import datetime
from pydantic import ValidationError
from src.ai_engine import VinHomeItem, VinFastItem

def test_vinhome_hashing_stability():
    """Test that same content results in same hash ID"""
    data = {
        "id": 1,
        "title": "Ocean Park",
        "description": "Lux",
        "item_type": "property",
        "price": 3000000,
        "created_at": datetime(2025, 1, 1),
        "total_area": 50.0,
        "num_rooms": 1,
        "direction": "S"
    }
    item1 = VinHomeItem(**data)
    item1.process()
    
    # Slight change in ID should NOT change doc_id if content is same
    data["id"] = 99
    item2 = VinHomeItem(**data)
    item2.process()
    
    assert item1.doc_id == item2.doc_id

def test_vinhome_validation_missing_fields():
    """Test Pydantic firewall catches missing fields"""
    with pytest.raises(ValidationError):
        VinHomeItem(id=1, title="Missing fields")

def test_weighted_embedding_logic():
    """Test that title is weighted correctly in vector_text"""
    item = VinFastItem(
        id=1,
        title="VF8",
        description="SUV",
        item_type="vehicle",
        price=1000,
        created_at=datetime(2025, 1, 1),
        version="Plus",
        color="Red",
        vehicle_type="Car"
    )
    item.process()
    # Title (VF8) weight is 3, Plus weight is 2
    assert "VF8 VF8 VF8" in item.vector_text
    assert "Plus Plus" in item.vector_text
    assert "SUV" in item.vector_text

def test_vinfast_hash_uniqueness():
    """Different versions should have different hashes"""
    base_data = {
        "id": 1,
        "title": "VF8",
        "item_type": "vehicle",
        "price": 1000,
        "created_at": datetime(2025, 1, 1),
        "color": "Red",
        "vehicle_type": "Car"
    }
    
    item_v1 = VinFastItem(**base_data, version="Plus")
    item_v1.process()
    
    item_v2 = VinFastItem(**base_data, version="Eco")
    item_v2.process()
    
    assert item_v1.doc_id != item_v2.doc_id
