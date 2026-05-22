# Order Service - REST API Endpoints

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid
from pydantic import BaseModel, Field, validator
from fastapi import APIRouter, HTTPException, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession

from database import (
    AsyncSessionLocal, Order, OrderRepository, OutboxRepository,
    OrderStatus
)

logger = logging.getLogger(__name__)

# ============================================
# Pydantic Models (Request/Response)
# ============================================

class OrderItem(BaseModel):
    """Order line item"""
    sku: str = Field(..., min_length=1, description="Product SKU")
    quantity: int = Field(..., gt=0, description="Quantity")
    price: float = Field(..., gt=0, description="Unit price")
    
    @validator('sku')
    def validate_sku(cls, v):
        if not v.strip():
            raise ValueError("SKU cannot be empty")
        return v.strip()

class CreateOrderRequest(BaseModel):
    """Request model for order creation"""
    user_id: str = Field(..., min_length=36, max_length=36, description="User UUID")
    items: List[OrderItem] = Field(..., min_items=1, description="Items to order")
    
    @validator('user_id')
    def validate_user_id(cls, v):
        try:
            uuid.UUID(v)
            return v
        except ValueError:
            raise ValueError("Invalid UUID format for user_id")
    
    @validator('items')
    def validate_items(cls, v):
        if not v:
            raise ValueError("At least one item is required")
        return v

class CreateOrderResponse(BaseModel):
    """Response model for order creation"""
    order_id: str
    status: str
    total_amount: float
    created_at: datetime
    
    class Config:
        from_attributes = True

class GetOrderResponse(BaseModel):
    """Response model for order retrieval"""
    order_id: str = Field(..., alias="id")
    user_id: str
    items: List[Dict[str, Any]]
    status: str
    total_amount: float
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class ErrorResponse(BaseModel):
    """Error response model"""
    error: str
    detail: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    request_id: Optional[str] = None

# ============================================
# API Router
# ============================================

router = APIRouter(
    prefix="/api/orders",
    tags=["orders"],
    responses={
        400: {"model": ErrorResponse, "description": "Bad Request"},
        404: {"model": ErrorResponse, "description": "Not Found"},
        500: {"model": ErrorResponse, "description": "Internal Server Error"},
    }
)

async def get_session() -> AsyncSession:
    """Get database session"""
    async with AsyncSessionLocal() as session:
        yield session

@router.post(
    "",
    response_model=CreateOrderResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a new order",
    description="Creates a new order and publishes an OrderCreated event via Kafka"
)
async def create_order(
    request: CreateOrderRequest,
    session: AsyncSession = Depends(get_session)
):
    """
    Create a new order
    
    This endpoint:
    1. Validates the request
    2. Creates the order with status=PENDING
    3. Adds OrderCreated event to the outbox table (Transactional Outbox pattern)
    4. Returns 202 Accepted
    
    The outbox publisher background task will pick up the event and publish to Kafka.
    This ensures atomicity - if the service crashes, events will be republished on restart.
    """
    request_id = str(uuid.uuid4())
    
    try:
        logger.info(
            f"[{request_id}] Creating order for user {request.user_id} "
            f"with {len(request.items)} items"
        )
        
        # Create order in database
        order = await OrderRepository.create_order(
            session,
            user_id=request.user_id,
            items=[item.dict() for item in request.items]
        )
        
        # Create OrderCreated event payload
        event_payload = {
            "order_id": order.id,
            "user_id": order.user_id,
            "items": order.items,
            "total_amount": float(order.total_amount),
            "created_at": order.created_at.isoformat()
        }
        
        # Add event to outbox (Transactional Outbox Pattern)
        # This ensures the event is stored in the same transaction as the order
        await OutboxRepository.add_event(
            session,
            aggregate_type="Order",
            aggregate_id=order.id,
            event_type="OrderCreated",
            payload=event_payload
        )
        
        # Commit everything together (atomicity)
        await session.commit()
        
        logger.info(
            f"[{request_id}] Order {order.id} created successfully. "
            f"Event added to outbox for publishing."
        )
        
        return CreateOrderResponse(
            order_id=order.id,
            status=order.status.value,
            total_amount=float(order.total_amount),
            created_at=order.created_at
        )
        
    except ValueError as e:
        logger.warning(f"[{request_id}] Validation error: {e}")
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"[{request_id}] Unexpected error creating order: {e}")
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@router.get(
    "/{order_id}",
    response_model=GetOrderResponse,
    status_code=status.HTTP_200_OK,
    summary="Get order details",
    description="Retrieves details of a specific order"
)
async def get_order(
    order_id: str,
    session: AsyncSession = Depends(get_session)
):
    """
    Get order by ID
    
    Returns:
    - 200: Order details
    - 404: Order not found
    """
    request_id = str(uuid.uuid4())
    
    try:
        # Validate UUID format
        try:
            uuid.UUID(order_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid order_id format"
            )
        
        logger.info(f"[{request_id}] Fetching order {order_id}")
        
        order = await OrderRepository.get_order(session, order_id)
        
        if not order:
            logger.warning(f"[{request_id}] Order {order_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order {order_id} not found"
            )
        
        logger.info(f"[{request_id}] Order {order_id} retrieved successfully")
        
        return GetOrderResponse(
            id=order.id,
            user_id=order.user_id,
            items=order.items,
            status=order.status.value,
            total_amount=float(order.total_amount),
            created_at=order.created_at,
            updated_at=order.updated_at
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error fetching order {order_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@router.get(
    "",
    response_model=List[GetOrderResponse],
    status_code=status.HTTP_200_OK,
    summary="List orders",
    description="Lists all orders for a specific user"
)
async def list_orders(
    user_id: str,
    limit: int = 50,
    offset: int = 0,
    session: AsyncSession = Depends(get_session)
):
    """
    List orders for a user
    
    Query Parameters:
    - user_id: User UUID (required)
    - limit: Max number of orders (default: 50, max: 100)
    - offset: Pagination offset (default: 0)
    
    Returns:
    - 200: List of orders
    - 400: Invalid parameters
    """
    request_id = str(uuid.uuid4())
    
    try:
        # Validate UUID format
        try:
            uuid.UUID(user_id)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid user_id format"
            )
        
        # Validate pagination parameters
        if limit < 1 or limit > 100:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="limit must be between 1 and 100"
            )
        
        if offset < 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="offset must be >= 0"
            )
        
        logger.info(f"[{request_id}] Listing orders for user {user_id} (limit={limit}, offset={offset})")
        
        orders = await OrderRepository.list_orders(
            session,
            user_id=user_id,
            limit=limit,
            offset=offset
        )
        
        logger.info(f"[{request_id}] Found {len(orders)} orders for user {user_id}")
        
        return [
            GetOrderResponse(
                id=order.id,
                user_id=order.user_id,
                items=order.items,
                status=order.status.value,
                total_amount=float(order.total_amount),
                created_at=order.created_at,
                updated_at=order.updated_at
            )
            for order in orders
        ]
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[{request_id}] Error listing orders: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Internal server error"
        )

@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="Health check",
    description="Health check endpoint for container orchestration"
)
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "order-service"}
