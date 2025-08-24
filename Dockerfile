FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1

COPY requirements.txt /app/
RUN pip install -r requirements.txt

COPY . /app
CMD ["streamlit", "run", "dashboard_app.py", "--server.address", "0.0.0.0", "--server.port", "8501"]
