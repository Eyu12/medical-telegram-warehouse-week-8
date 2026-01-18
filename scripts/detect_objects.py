import os
import pandas as pd
from ultralytics import YOLO
from datetime import datetime

# -----------------------------
# Configuration
# -----------------------------
MODEL_NAME = "yolov8n.pt"
CONF_THRESHOLD = 0.15

IMAGES_BASE_DIR = "data/raw/images"
MESSAGES_CSV = "data/raw/telegram_messages/2026-01-16/telegram_messages.csv"
OUTPUT_CSV = "data/enriched/image_detections.csv"

IMAGE_EXTENSIONS = (".jpg", ".jpeg", ".png")

# -----------------------------
# Helper: Build Image Index
# -----------------------------
def build_image_index(base_dir):
    index = {}
    for root, _, files in os.walk(base_dir):
        for f in files:
            if f.lower().endswith(IMAGE_EXTENSIONS):
                index[f] = os.path.join(root, f)
    return index

# -----------------------------
# Load YOLOv8 Model
# -----------------------------
print("Loading YOLOv8 model...")
model = YOLO(MODEL_NAME)

# -----------------------------
# Load Message Metadata
# -----------------------------
print("Loading message metadata...")
messages_df = pd.read_csv(MESSAGES_CSV)

# Detect image column dynamically
image_column = None
for col in ["media_file", "file_path"]:
    if col in messages_df.columns:
        image_column = col
        break

if image_column is None:
    raise ValueError("No image column found (media_file or file_path)")

print(f"Using image column: {image_column}")

messages_df = messages_df.dropna(subset=[image_column])

# -----------------------------
# Build Image Lookup Index
# -----------------------------
print("Indexing images...")
image_index = build_image_index(IMAGES_BASE_DIR)
print(f"Indexed {len(image_index)} images")

# -----------------------------
# Run Object Detection
# -----------------------------
detections = []

for _, row in messages_df.iterrows():
    raw_value = str(row[image_column])

    # Skip videos / documents
    if not raw_value.lower().endswith(IMAGE_EXTENSIONS):
        continue

    filename = os.path.basename(raw_value)

    image_path = image_index.get(filename)

    if image_path is None:
        continue

    results = model(image_path, conf=CONF_THRESHOLD, verbose=False)

    for result in results:
        if result.boxes is None:
            continue

        for box in result.boxes:
            detections.append({
                "message_id": row["message_id"],
                "channel_username": row.get("channel_username"),
                "image_path": image_path,
                "detected_object": model.names[int(box.cls)],
                "confidence": float(box.conf),
                "detected_at": datetime.utcnow()
            })

# -----------------------------
# Save Results
# -----------------------------
detections_df = pd.DataFrame(detections)
os.makedirs(os.path.dirname(OUTPUT_CSV), exist_ok=True)

if detections_df.empty:
    print("No detections found.")
else:
    detections_df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved {len(detections_df)} detections to {OUTPUT_CSV}")
