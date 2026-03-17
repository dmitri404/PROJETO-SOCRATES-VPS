FROM mcr.microsoft.com/playwright/python:v1.57.0-noble
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN playwright install chromium --with-deps
COPY . .
CMD ["python", "-u", "novo7.py"]
