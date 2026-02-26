from .health import router as health_router
from .inputs import router as inputs_router
from .patients import router as patients_router

routes = [
    health_router,
    inputs_router,
    patients_router,
]