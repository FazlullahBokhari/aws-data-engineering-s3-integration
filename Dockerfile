FROM eclipse-temurin:17-jdk

WORKDIR /app

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    && rm -rf /var/lib/apt/lists/*

# Copy only requirements first (better Docker caching)
COPY requirements.txt .

# Install Python deps
RUN pip3 install --break-system-packages -r requirements.txt

# Copy rest of the code
COPY . .