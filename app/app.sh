#!/bin/bash

find /app -name "*.sh" -exec sed -i 's/\r$//' {} +
find /app -name "*.py" -exec sed -i 's/\r$//' {} +

echo "=========================================="
echo "Simple Search Engine - BM25"
echo "=========================================="

service ssh restart

echo ""
echo "Step 1: Starting services..."
bash /app/start-services.sh

echo ""
echo "Step 2: Setting up Python environment..."
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt

venv-pack -o .venv.tar.gz --force

echo ""
echo "Step 3: Preparing data..."
bash /app/prepare_data.sh

echo ""
echo "Step 4: Running indexer..."
bash /app/index.sh

echo ""
echo "Step 5: Running search queries..."
echo ""
echo "--- Query 1 ---"
bash /app/search.sh "machine learning algorithms"
echo ""
echo "--- Query 2 ---"
bash /app/search.sh "history of ancient civilizations"
echo ""
echo "--- Query 3 ---"
bash /app/search.sh "climate change effects"

echo ""
echo "=========================================="
echo "All tasks completed!"
echo "=========================================="

echo "Container is ready. Use 'docker exec -it cluster-master bash' to run interactive queries."
tail -f /dev/null
