from typing import Annotated
from sqlalchemy.orm import Session
from fastapi import FastAPI, Depends, HTTPException, Path
from pydantic import BaseModel, Field
import models
from models import Product
from database import engine, SessionLocal

# init FastAPI
app = FastAPI()

# tables creation
models.Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

db_dependency = Annotated[Session, Depends(get_db)]

# Pydantic model for validation
class ProductRequest(BaseModel):
    name: str = Field(min_length=1)
    description: str = Field(min_length=3, max_length=500)
    price: float = Field(gt=0)
    is_available: bool

# CRUD operations

# Endpoint to read all products
@app.get('/products', status_code=200)
async def read_all_products(db: db_dependency):
    return db.query(Product).all()

# Endpoint to get a product by its ID
@app.get('/product/{product_id}', status_code=200)
async def get_product_by_id(db: db_dependency, product_id: int = Path(gt=0)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if product:
        return product
    raise HTTPException(status_code=404, detail='Product not found') # If the product is not found, return a 404 error

# Endpoint to create a new product
@app.post('/product', status_code=201)
async def create_product(db: db_dependency, product_request: ProductRequest):
    new_product = Product(**product_request.dict())
    db.add(new_product)
    db.commit()
    return new_product

# Endpoint to update an existing product
@app.put('/product/{product_id}', status_code=204)
async def update_product(db: db_dependency, product_request: ProductRequest, product_id: int = Path(gt=0)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail='Product not found')
    
    # Updates the product fields with new values
    product.name = product_request.name
    product.description = product_request.description
    product.price = product_request.price
    product.is_available = product_request.is_available

    db.commit()

# Endpoint to delete a product by ID
@app.delete('/product/{product_id}', status_code=204)
async def delete_product(db: db_dependency, product_id: int = Path(gt=0)):
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail='Product not found')
    
    # Deletes the product from the database
    db.delete(product)
    db.commit()
