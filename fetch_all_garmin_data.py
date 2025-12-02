#!/usr/bin/env python3
"""
OPTIMIZED: Fetch ALL health data from Garmin Connect with parallel requests.
~10x faster than sequential version using concurrent.futures.
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from garminconnect import Garmin
import time

# Load credentials
load_dotenv()
EMAIL = os.getenv("GARMIN_EMAIL")
PASSWORD = os.getenv("GARMIN_PASSWORD")

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION_FILE = Path(__file__).parent / ".garmin_session"

# Rate limiting: max requests per second
MAX_WORKERS = 10  # Parallel threads
REQUEST_DELAY = 0.1  # Seconds between requests


def get_client():
    """Initialize Garmin client with session caching"""
    client = Garmin(EMAIL, PASSWORD)

    if SESSION_FILE.exists():
        try:
            client.login(str(SESSION_FILE))
            print("Logged in using saved session")
            return client
        except Exception as e:
            print(f"Session expired, re-authenticating: {e}")

    client.login()
    client.garth.dump(str(SESSION_FILE))
    print("Fresh login successful")
    return client


def safe_call(func, *args, default=None, **kwargs):
    """Safely call API method with retry"""
    for attempt in range(3):
        try:
            result = func(*args, **kwargs)
            return result
        except Exception as e:
            if attempt < 2:
                time.sleep(1)  # Wait before retry
            else:
                return default


def fetch_day_data(client, date_str, data_type):
    """Fetch specific data type for a single day"""
    time.sleep(REQUEST_DELAY)  # Rate limiting

    try:
        if data_type == "stats":
            return safe_call(client.get_stats, date_str, default={})
        elif data_type == "sleep":
            sleep = safe_call(client.get_sleep_data, date_str, default=None)
            if sleep and sleep.get("dailySleepDTO"):
                result = sleep["dailySleepDTO"]
                result["sleep_levels"] = sleep.get("sleepLevels", [])
                return result
            return None
        elif data_type == "heart_rate":
            return safe_call(client.get_heart_rates, date_str, default=None)
        elif data_type == "hrv":
            return safe_call(client.get_hrv_data, date_str, default=None)
        elif data_type == "stress":
            return safe_call(client.get_stress_data, date_str, default=None)
        elif data_type == "respiration":
            return safe_call(client.get_respiration_data, date_str, default=None)
        elif data_type == "spo2":
            return safe_call(client.get_spo2_data, date_str, default=None)
        elif data_type == "floors":
            return safe_call(client.get_floors, date_str, default=None)
        elif data_type == "hydration":
            return safe_call(client.get_hydration_data, date_str, default=None)
        elif data_type == "intensity":
            return safe_call(client.get_intensity_minutes_data, date_str, default=None)
    except Exception as e:
        return None
    return None


def fetch_history_parallel(client, dates, data_type):
    """Fetch history data in parallel"""
    results = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(fetch_day_data, client, date_str, data_type): date_str
            for date_str in dates
        }

        for future in as_completed(futures):
            date_str = futures[future]
            try:
                result = future.result()
                if result:
                    result["date"] = date_str
                    results.append(result)
            except Exception:
                pass

    # Sort by date descending
    results.sort(key=lambda x: x.get("date", ""), reverse=True)
    return results


def fetch_all_data(days_history=30):
    """Fetch ALL available health data with parallel requests"""
    client = get_client()
    today = datetime.now().date()
    start_date = today - timedelta(days=days_history)

    # Generate date list
    dates = [(today - timedelta(days=i)).isoformat() for i in range(days_history + 1)]

    print(f"\n=== Fetching Garmin data from {start_date} to {today} ({len(dates)} days) ===")
    print(f"Using {MAX_WORKERS} parallel workers\n")

    data = {
        "fetched_at": datetime.now().isoformat(),
        "date_range": {
            "start": str(start_date),
            "end": str(today),
            "days": days_history
        }
    }

    # =============================================
    # USER PROFILE (single call)
    # =============================================
    print("Fetching user profile...")
    data["user_profile"] = safe_call(client.get_user_profile, default={})
    data["devices"] = safe_call(client.get_devices, default=[])

    # =============================================
    # BULK METHODS (use date range where available)
    # =============================================
    start_str = start_date.isoformat()
    end_str = today.isoformat()

    print("Fetching bulk data (steps, body battery, body composition)...")
    data["daily_steps_bulk"] = safe_call(client.get_daily_steps, start_str, end_str, default=[])
    data["body_battery_bulk"] = safe_call(client.get_body_battery, start_str, end_str, default=[])
    data["body_composition"] = safe_call(client.get_body_composition, start_str, end_str, default={})
    data["weight_history"] = safe_call(client.get_weigh_ins, start_str, end_str, default=[])
    data["blood_pressure"] = safe_call(client.get_blood_pressure, start_str, end_str, default=[])
    data["hill_score"] = safe_call(client.get_hill_score, start_str, end_str, default={})
    data["endurance_score"] = safe_call(client.get_endurance_score, start_str, end_str, default={})

    # =============================================
    # PARALLEL FETCHING for day-by-day data
    # =============================================

    # Stats (parallel)
    print(f"Fetching daily stats ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["daily_stats"] = fetch_history_parallel(client, dates, "stats")
    print(f"  Got {len(data['daily_stats'])} days in {time.time() - start_time:.1f}s")

    # Sleep (parallel)
    print(f"Fetching sleep data ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["sleep_history"] = fetch_history_parallel(client, dates, "sleep")
    print(f"  Got {len(data['sleep_history'])} nights in {time.time() - start_time:.1f}s")

    # Heart Rate (parallel)
    print(f"Fetching heart rate data ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["heart_rate_history"] = fetch_history_parallel(client, dates, "heart_rate")
    print(f"  Got {len(data['heart_rate_history'])} days in {time.time() - start_time:.1f}s")

    # HRV (parallel)
    print(f"Fetching HRV data ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["hrv_history"] = fetch_history_parallel(client, dates, "hrv")
    print(f"  Got {len(data['hrv_history'])} days in {time.time() - start_time:.1f}s")

    # Stress (parallel)
    print(f"Fetching stress data ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["stress_history"] = fetch_history_parallel(client, dates, "stress")
    print(f"  Got {len(data['stress_history'])} days in {time.time() - start_time:.1f}s")

    # Respiration (parallel)
    print(f"Fetching respiration data ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["respiration_history"] = fetch_history_parallel(client, dates, "respiration")
    print(f"  Got {len(data['respiration_history'])} days in {time.time() - start_time:.1f}s")

    # SpO2 (parallel)
    print(f"Fetching SpO2 data ({len(dates)} days, parallel)...")
    start_time = time.time()
    data["spo2_history"] = fetch_history_parallel(client, dates, "spo2")
    print(f"  Got {len(data['spo2_history'])} days in {time.time() - start_time:.1f}s")

    # =============================================
    # ACTIVITIES (single bulk call)
    # =============================================
    print("Fetching ALL activities...")
    start_time = time.time()
    all_activities = []
    offset = 0
    batch_size = 1000
    while True:
        batch = safe_call(client.get_activities, offset, batch_size, default=[])
        if not batch:
            break
        all_activities.extend(batch)
        print(f"  Fetched {len(all_activities)} activities so far...")
        offset += len(batch)
        if len(batch) < batch_size:
            break
    data["activities"] = all_activities
    print(f"  Got {len(data.get('activities', []))} TOTAL activities in {time.time() - start_time:.1f}s")

    data["activity_types"] = safe_call(client.get_activity_types, default=[])

    # =============================================
    # TRAINING STATUS
    # =============================================
    print("Fetching training status...")
    data["training_status"] = safe_call(client.get_training_status, today.isoformat(), default={})
    data["training_readiness"] = safe_call(client.get_training_readiness, today.isoformat(), default={})
    data["max_metrics"] = safe_call(client.get_max_metrics, today.isoformat(), default={})
    data["fitness_age"] = safe_call(client.get_fitnessage_data, today.isoformat(), default={})
    data["race_predictions"] = safe_call(client.get_race_predictions, default={})
    data["personal_records"] = safe_call(client.get_personal_record, default={})

    # =============================================
    # OTHER
    # =============================================
    print("Fetching goals, badges, gear...")
    data["goals"] = safe_call(client.get_goals, "all", default={})
    data["earned_badges"] = safe_call(client.get_earned_badges, default=[])
    data["badge_challenges"] = safe_call(client.get_badge_challenges, 0, 100, default=[])
    data["gear"] = safe_call(client.get_gear, "", default=[])
    data["workouts"] = safe_call(client.get_workouts, default=[])

    # =============================================
    # SAVE DATA
    # =============================================

    # Save FULL data to JSON (local only, for backup)
    json_file = DATA_DIR / "garmin_full_data.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    file_size_mb = json_file.stat().st_size / 1024 / 1024
    print(f"\n=== Full data saved to {json_file} ===")
    print(f"File size: {file_size_mb:.2f} MB")

    # Save SPLIT files for GitHub (better for RAG/Knowledge Base)
    save_split_files(data)

    return data


def save_split_files(data):
    """Save data split into separate files by type for better RAG indexing"""
    print("\n=== Saving split files for GitHub ===")

    split_dir = DATA_DIR / "split"
    split_dir.mkdir(exist_ok=True)

    # Metadata for all files
    metadata = {
        "fetched_at": data.get("fetched_at"),
        "date_range": data.get("date_range"),
    }

    files_saved = []

    # 1. Activities (largest, most important)
    if data.get("activities"):
        file_path = split_dir / "activities.json"
        content = {
            **metadata,
            "total_count": len(data["activities"]),
            "activities": data["activities"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"activities.json: {size_mb:.2f} MB ({len(data['activities'])} activities)")

    # 2. Daily Stats
    if data.get("daily_stats"):
        file_path = split_dir / "daily_stats.json"
        content = {
            **metadata,
            "total_days": len(data["daily_stats"]),
            "daily_stats": data["daily_stats"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"daily_stats.json: {size_mb:.2f} MB ({len(data['daily_stats'])} days)")

    # 3. Sleep History
    if data.get("sleep_history"):
        file_path = split_dir / "sleep_history.json"
        content = {
            **metadata,
            "total_nights": len(data["sleep_history"]),
            "sleep_history": data["sleep_history"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"sleep_history.json: {size_mb:.2f} MB ({len(data['sleep_history'])} nights)")

    # 4. Heart Rate History
    if data.get("heart_rate_history"):
        file_path = split_dir / "heart_rate_history.json"
        content = {
            **metadata,
            "total_days": len(data["heart_rate_history"]),
            "heart_rate_history": data["heart_rate_history"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"heart_rate_history.json: {size_mb:.2f} MB ({len(data['heart_rate_history'])} days)")

    # 5. Stress History
    if data.get("stress_history"):
        file_path = split_dir / "stress_history.json"
        content = {
            **metadata,
            "total_days": len(data["stress_history"]),
            "stress_history": data["stress_history"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"stress_history.json: {size_mb:.2f} MB ({len(data['stress_history'])} days)")

    # 6. HRV History
    if data.get("hrv_history"):
        file_path = split_dir / "hrv_history.json"
        content = {
            **metadata,
            "total_days": len(data["hrv_history"]),
            "hrv_history": data["hrv_history"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_kb = file_path.stat().st_size / 1024
        files_saved.append(f"hrv_history.json: {size_kb:.1f} KB ({len(data['hrv_history'])} days)")

    # 7. SpO2 History
    if data.get("spo2_history"):
        file_path = split_dir / "spo2_history.json"
        content = {
            **metadata,
            "total_days": len(data["spo2_history"]),
            "spo2_history": data["spo2_history"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"spo2_history.json: {size_mb:.2f} MB ({len(data['spo2_history'])} days)")

    # 8. Respiration History
    if data.get("respiration_history"):
        file_path = split_dir / "respiration_history.json"
        content = {
            **metadata,
            "total_days": len(data["respiration_history"]),
            "respiration_history": data["respiration_history"]
        }
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(content, f, indent=2, ensure_ascii=False, default=str)
        size_mb = file_path.stat().st_size / 1024 / 1024
        files_saved.append(f"respiration_history.json: {size_mb:.2f} MB ({len(data['respiration_history'])} days)")

    # 9. Training & Profile (small, combined)
    profile_data = {
        **metadata,
        "user_profile": data.get("user_profile", {}),
        "devices": data.get("devices", []),
        "training_status": data.get("training_status", {}),
        "training_readiness": data.get("training_readiness", {}),
        "max_metrics": data.get("max_metrics", {}),
        "fitness_age": data.get("fitness_age", {}),
        "race_predictions": data.get("race_predictions", {}),
        "personal_records": data.get("personal_records", {}),
        "goals": data.get("goals", {}),
        "earned_badges": data.get("earned_badges", []),
        "body_composition": data.get("body_composition", {}),
        "weight_history": data.get("weight_history", []),
    }
    file_path = split_dir / "profile_and_training.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(profile_data, f, indent=2, ensure_ascii=False, default=str)
    size_kb = file_path.stat().st_size / 1024
    files_saved.append(f"profile_and_training.json: {size_kb:.1f} KB")

    # Print summary
    print(f"Split files saved to {split_dir}/")
    for f in files_saved:
        print(f"  - {f}")

    # Calculate total size
    total_size = sum(f.stat().st_size for f in split_dir.glob("*.json"))
    print(f"Total split files size: {total_size / 1024 / 1024:.2f} MB")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch Garmin health data (optimized)")
    parser.add_argument("--days", type=int, default=30, help="Days of history to fetch")
    args = parser.parse_args()

    total_start = time.time()
    fetch_all_data(days_history=args.days)
    print(f"\n=== TOTAL TIME: {time.time() - total_start:.1f} seconds ===")
