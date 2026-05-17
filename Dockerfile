FROM python:3.11-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-21-jre-headless && \
    rm -rf /var/lib/apt/lists/* && \
    ln -sf /usr/lib/jvm/java-21-openjdk-* /usr/lib/jvm/java-21

ENV JAVA_HOME=/usr/lib/jvm/java-21
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ src/

CMD ["python", "src/benchmark.py", \
     "--seed-url", "https://github.com/apache/spark", \
     "--keyword", "apache", \
     "--max-depth", "2", \
     "--max-urls", "50", \
     "--cores", "1,2,4,8", \
     "--runs", "3", \
     "--output-dir", "results"]
