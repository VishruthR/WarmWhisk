set -e

BASE_URL="https://picsum.photos"
START_INDEX=1
END_INDEX=30

for ((i=$START_INDEX; i<$END_INDEX; i++)); do
    echo "Downloading image $i"
    curl -L -s -o "image_$i.jpg" "$BASE_URL/id/$i/300"
done