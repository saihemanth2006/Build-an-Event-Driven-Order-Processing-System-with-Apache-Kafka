# Order Service - Main Application

import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import app_config
from database import create_tables
from kafka_producer import kafka_producer
from outbox_publisher import outbox_publisher
from api import router as orders_router

# Configure logging
logging.basicConfig(
    level=getattr(logging, app_config.log_level),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================
# Application Lifecycle Events
# ============================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle application startup and shutdown"""
    
    # Startup
    logger.info("=" * 60)
    logger.info(f"Starting {app_config.app_name} v{app_config.version}")
    logger.info("=" * 60)
    
    try:
        # Create database tables
        await create_tables()
        logger.info("✓ Database tables initialized")
        
        # Start Kafka producer
        await kafka_producer.start()
        logger.info("✓ Kafka Producer started")
        
        # Start outbox publisher (background task)
        await outbox_publisher.start()
        logger.info("✓ Outbox Publisher started")
        
        logger.info("=" * 60)
        logger.info("Application startup completed successfully")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("=" * 60)
    logger.info("Shutting down application...")
    logger.info("=" * 60)
    
    try:
        await outbox_publisher.stop()
        logger.info("✓ Outbox Publisher stopped")
        
        await kafka_producer.stop()
        logger.info("✓ Kafka Producer stopped")
        
        logger.info("=" * 60)
        logger.info("Application shutdown completed")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")

# ============================================
# Create FastAPI Application
# ============================================

app = FastAPI(
    title=app_config.app_name,
    version=app_config.version,
    description="Event-Driven Order Processing System - Order Service",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
    openapi_url="/api/openapi.json"
)

# ============================================
# CORS Middleware
# ============================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================
# Include Routers
# ============================================

app.include_router(orders_router)

# ============================================
# Root Endpoint
# ============================================

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": app_config.app_name,
        "version": app_config.version,
        "status": "running",
        "docs": "/api/docs"
    }

@app.get("/health")
async def health():
    """Health check endpoint"""
    return {"status": "healthy", "service": app_config.app_name}

# ============================================
# Exception Handlers
# ============================================

@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {exc}")
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"}
    )

if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=app_config.api_port,
        log_level=app_config.log_level.lower()
    )
