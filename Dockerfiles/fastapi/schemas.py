from pydantic import BaseModel
from typing import Optional
from datetime import date

class CoverType(BaseModel):

    brokered_by: float = 101640.0
    stats: str = 'for_sale'
    bed: float = 4.0
    bath: float = 2.0
    acre_lot: float = 0.38
    street: float = 1758218.0
    state: str = 'Massachusetts'
    zip_code: float = 6016.0
    house_size: float = 1617.0
