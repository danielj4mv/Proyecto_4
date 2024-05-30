from pydantic import BaseModel
from typing import Optional
from datetime import date

class CoverType(BaseModel):

    brokered_by: float = 101640.0
    bed: float = 4.0
    bath: float = 2.0
    acre_lot: float = 0.38
    street: float = 1758218.0
    zip_code: float = 6016.0
    house_size: float = 1617.0
    prev_sold_date: int = 427
    years_since_sold: float = 25.0
