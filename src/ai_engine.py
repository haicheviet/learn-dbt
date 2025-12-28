
import duckdb
import hashlib
import json
from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError

# --- 1. Data Contracts (Pydantic Firewall) ---


from datetime import datetime

class BaseItem(BaseModel):
    id: int
    title: str
    description: Optional[str] = ""
    item_type: str
    price: Optional[int] = 0
    created_at: datetime
    
    # Enriched fields
    doc_id: Optional[str] = None
    vector_text: Optional[str] = None

    def generate_hash(self, sensitive_fields: List[str]) -> str:
        """Content-based hashing (SHA-256) for deduplication/id stability"""
        data_str = "".join([str(getattr(self, f)) for f in sensitive_fields])
        return hashlib.sha256(data_str.encode()).hexdigest()

    def calculate_weighted_text(self, weights: dict) -> str:
        """Weighted embedding text generation"""
        parts = []
        for field, weight in weights.items():
            val = getattr(self, field, "")
            parts.extend([str(val)] * weight) # Repeat text by weight
        return " ".join(parts)

class VinHomeItem(BaseItem):
    total_area: Optional[float]
    num_rooms: Optional[int]
    direction: Optional[str]

    def process(self):
        # Sensitive fields for VinHome: title, total_area, num_rooms, direction, price
        self.doc_id = self.generate_hash(['title', 'total_area', 'num_rooms', 'direction', 'price'])
        
        # Weighted Embedding: title (x3), description (x1)
        self.vector_text = self.calculate_weighted_text({'title': 3, 'description': 1})

class VinFastItem(BaseItem):
    version: Optional[str]
    color: Optional[str]
    vehicle_type: Optional[str]

    def process(self):
        # Sensitive: title, version, color, vehicle_type, price
        self.doc_id = self.generate_hash(['title', 'version', 'color', 'vehicle_type', 'price'])
        
        # Weighted: title (x3), version (x2), description (x1)
        self.vector_text = self.calculate_weighted_text({'title': 3, 'version': 2, 'description': 1})


# --- 2. AI Service Logic ---

def process_data(db_path: str = 'learn_dbt.duckdb'):
    con = duckdb.connect(db_path)
    
    # Process VinHome
    print("--- Processing VinHome ---")
    vh_df = con.sql("SELECT * FROM dim_vinhome").df()
    vh_items = []
    for _, row in vh_df.iterrows():
        try:
            item = VinHomeItem(**row.to_dict())
            item.process()
            vh_items.append(item.model_dump())
        except ValidationError as e:
            print(f"DLQ (Validation Failed): {row['id']} - {e}")

    # Process VinFast
    print("--- Processing VinFast ---")
    vf_df = con.sql("SELECT * FROM dim_vinfast").df()
    vf_items = []
    for _, row in vf_df.iterrows():
        try:
            item = VinFastItem(**row.to_dict())
            item.process()
            vf_items.append(item.model_dump())
        except ValidationError as e:
            print(f"DLQ (Validation Failed): {row['id']} - {e}")
            
    # Output to JSON (Mock Qdrant Ingestion)
    all_items = vh_items + vf_items
    with open('ready_to_index.json', 'w') as f:
        json.dump(all_items, f, indent=2, default=str)
    
    print(f"Successfully processed {len(all_items)} items. Output written to ready_to_index.json")

if __name__ == "__main__":
    process_data()
