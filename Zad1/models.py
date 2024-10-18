from database import Base
from sqlalchemy import Column, Integer, String, Float, Boolean

class Product(Base):
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    description = Column(String)
    price = Column(Float)
    is_available = Column(Boolean, default=True)
