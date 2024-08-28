from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from pangeia.ai_integration.agents import execute_analysis

app = FastAPI()


class QueryRequest(BaseModel):
    question: str


@app.post("/execute_query/")
async def execute_query(request: QueryRequest):
    try:
        result = execute_analysis(request.question)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Ponto de entrada para rodar a API
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
