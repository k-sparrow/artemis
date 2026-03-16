from langserve import add_routes
from fastapi import FastAPI

app = FastAPI(title="Chat endpoint", description="Chat endpoint using langserve")


add_routes(app)
