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

    # Save COMPACT summary for quick reference
    compact_data = generate_compact_data(data)
    compact_file = DATA_DIR / "garmin_health.json"
    with open(compact_file, "w", encoding="utf-8") as f:
        json.dump(compact_data, f, indent=2, ensure_ascii=False, default=str)

    print(f"Compact summary saved to {compact_file}")
    print(f"Compact file size: {compact_file.stat().st_size / 1024:.2f} KB")

    # Generate markdown summary
    generate_summary(data)

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


def generate_compact_data(data):
    """Generate compact JSON for GitHub with essential data only"""
    compact = {
        "fetched_at": data.get("fetched_at"),
        "date_range": data.get("date_range"),
        "total_activities": len(data.get("activities", [])),
        "oldest_activity": None,
        "newest_activity": None,
    }

    activities = data.get("activities", [])
    if activities:
        compact["newest_activity"] = activities[0].get("startTimeLocal", "")[:10]
        compact["oldest_activity"] = activities[-1].get("startTimeLocal", "")[:10]

    # Today's stats
    if data.get("daily_stats") and len(data["daily_stats"]) > 0:
        today = data["daily_stats"][0]
        compact["today"] = {
            "date": today.get("date"),
            "steps": today.get("totalSteps"),
            "distance_km": round(today.get("totalDistanceMeters", 0) / 1000, 2) if today.get("totalDistanceMeters") else 0,
            "calories": today.get("totalKilocalories"),
            "active_calories": today.get("activeKilocalories"),
            "resting_hr": today.get("restingHeartRate"),
            "min_hr": today.get("minHeartRate"),
            "max_hr": today.get("maxHeartRate"),
            "stress_avg": today.get("averageStressLevel"),
            "body_battery_high": today.get("bodyBatteryChargedValue"),
            "body_battery_low": today.get("bodyBatteryDrainedValue"),
            "floors_climbed": today.get("floorsAscended"),
        }

    # Last sleep
    if data.get("sleep_history") and len(data["sleep_history"]) > 0:
        sleep = data["sleep_history"][0]
        compact["last_sleep"] = {
            "date": sleep.get("date"),
            "duration_hours": round(sleep.get("sleepTimeSeconds", 0) / 3600, 1),
            "deep_hours": round(sleep.get("deepSleepSeconds", 0) / 3600, 1),
            "light_hours": round(sleep.get("lightSleepSeconds", 0) / 3600, 1),
            "rem_hours": round(sleep.get("remSleepSeconds", 0) / 3600, 1),
            "awake_hours": round(sleep.get("awakeSleepSeconds", 0) / 3600, 1),
            "sleep_score": sleep.get("sleepScores", {}).get("overall", {}).get("value"),
        }

    # HRV
    if data.get("hrv_history") and len(data["hrv_history"]) > 0:
        hrv = data["hrv_history"][0]
        summary = hrv.get("hrvSummary", {})
        compact["hrv"] = {
            "weekly_avg": summary.get("weeklyAvg"),
            "last_night": summary.get("lastNight"),
            "status": summary.get("status"),
        }

    # Recent activities (last 20)
    compact["recent_activities"] = []
    for act in activities[:20]:
        compact["recent_activities"].append({
            "date": act.get("startTimeLocal", "")[:10],
            "name": act.get("activityName"),
            "type": act.get("activityType", {}).get("typeKey"),
            "duration_min": round(act.get("duration", 0) / 60, 1),
            "distance_km": round(act.get("distance", 0) / 1000, 2),
            "calories": act.get("calories"),
            "avg_hr": act.get("averageHR"),
            "max_hr": act.get("maxHR"),
        })

    # Activity summary by type
    activity_summary = {}
    for act in activities:
        act_type = act.get("activityType", {}).get("typeKey", "unknown")
        if act_type not in activity_summary:
            activity_summary[act_type] = {
                "count": 0,
                "total_duration_hours": 0,
                "total_distance_km": 0,
                "total_calories": 0,
            }
        activity_summary[act_type]["count"] += 1
        activity_summary[act_type]["total_duration_hours"] += act.get("duration", 0) / 3600
        activity_summary[act_type]["total_distance_km"] += act.get("distance", 0) / 1000
        activity_summary[act_type]["total_calories"] += act.get("calories", 0) or 0

    for act_type in activity_summary:
        activity_summary[act_type]["total_duration_hours"] = round(
            activity_summary[act_type]["total_duration_hours"], 1
        )
        activity_summary[act_type]["total_distance_km"] = round(
            activity_summary[act_type]["total_distance_km"], 1
        )

    compact["activity_summary"] = activity_summary

    # Training status
    if data.get("training_status"):
        ts = data["training_status"]
        compact["training"] = {
            "vo2_max": ts.get("vo2MaxPreciseValue"),
            "training_load": ts.get("acuteTrainingLoad"),
            "recovery_hours": round(ts.get("recoveryTimeInMinutes", 0) / 60, 0),
        }

    compact["personal_records"] = data.get("personal_records", [])

    # Data availability
    compact["data_availability"] = {
        "daily_stats_days": len(data.get("daily_stats", [])),
        "sleep_nights": len(data.get("sleep_history", [])),
        "heart_rate_days": len(data.get("heart_rate_history", [])),
        "hrv_days": len(data.get("hrv_history", [])),
        "stress_days": len(data.get("stress_history", [])),
        "spo2_days": len(data.get("spo2_history", [])),
        "respiration_days": len(data.get("respiration_history", [])),
        "activities_total": len(data.get("activities", [])),
        "weight_entries": len(data.get("weight_history", [])),
        "badges_earned": len(data.get("earned_badges", [])),
    }

    return compact


def generate_summary(data):
    """Generate a human-readable summary"""
    summary_file = DATA_DIR / "health_summary.md"

    lines = [
        "# Garmin Health Data Summary",
        "",
        f"**Data fetched:** {data.get('fetched_at', 'Unknown')[:16].replace('T', ' ')}",
        f"**Date range:** {data['date_range']['start']} to {data['date_range']['end']} ({data['date_range']['days']} days)",
        "",
    ]

    if data.get("user_profile"):
        profile = data["user_profile"]
        lines.extend([
            "## User Profile",
            f"- **Name:** {profile.get('displayName', 'N/A')}",
            f"- **Email:** {profile.get('userName', 'N/A')}",
            ""
        ])

    if data.get("daily_stats") and len(data["daily_stats"]) > 0:
        today_stats = data["daily_stats"][0]
        lines.extend([
            "## Today's Stats",
            f"- **Steps:** {today_stats.get('totalSteps', 'N/A'):,}",
            f"- **Distance:** {(today_stats.get('totalDistanceMeters', 0) or 0) / 1000:.2f} km",
            f"- **Calories:** {today_stats.get('totalKilocalories', 'N/A')}",
            f"- **Active Calories:** {today_stats.get('activeKilocalories', 'N/A')}",
            f"- **Resting HR:** {today_stats.get('restingHeartRate', 'N/A')} bpm",
            f"- **Min/Max HR:** {today_stats.get('minHeartRate', 'N/A')}/{today_stats.get('maxHeartRate', 'N/A')} bpm",
            f"- **Stress Level:** {today_stats.get('averageStressLevel', 'N/A')}",
            f"- **Body Battery:** {today_stats.get('bodyBatteryDrainedValue', 'N/A')} - {today_stats.get('bodyBatteryChargedValue', 'N/A')}",
            ""
        ])

    if data.get("sleep_history") and len(data["sleep_history"]) > 0:
        sleep = data["sleep_history"][0]
        duration_h = sleep.get("sleepTimeSeconds", 0) / 3600
        deep_h = sleep.get("deepSleepSeconds", 0) / 3600
        light_h = sleep.get("lightSleepSeconds", 0) / 3600
        rem_h = sleep.get("remSleepSeconds", 0) / 3600
        awake_h = sleep.get("awakeSleepSeconds", 0) / 3600
        score = sleep.get("sleepScores", {}).get("overall", {}).get("value", "N/A")

        lines.extend([
            "## Last Night's Sleep",
            f"- **Total Sleep:** {duration_h:.1f} hours",
            f"- **Deep Sleep:** {deep_h:.1f} hours",
            f"- **Light Sleep:** {light_h:.1f} hours",
            f"- **REM Sleep:** {rem_h:.1f} hours",
            f"- **Awake Time:** {awake_h:.1f} hours",
            f"- **Sleep Score:** {score}",
            ""
        ])

    if data.get("hrv_history") and len(data["hrv_history"]) > 0:
        hrv = data["hrv_history"][0]
        summary = hrv.get("hrvSummary", {})
        lines.extend([
            "## Heart Rate Variability (HRV)",
            f"- **Last Night:** {summary.get('lastNight', 'N/A')} ms",
            f"- **Weekly Average:** {summary.get('weeklyAvg', 'N/A')} ms",
            f"- **Status:** {summary.get('status', 'N/A')}",
            ""
        ])

    if data.get("training_status"):
        ts = data["training_status"]
        lines.extend([
            "## Training Status",
            f"- **VO2 Max:** {ts.get('vo2MaxPreciseValue', 'N/A')}",
            f"- **Training Load:** {ts.get('acuteTrainingLoad', 'N/A')}",
            f"- **Recovery Time:** {(ts.get('recoveryTimeInMinutes', 0) or 0) / 60:.0f} hours",
            ""
        ])

    if data.get("activities"):
        lines.extend(["## Recent Activities", ""])
        for act in data["activities"][:10]:
            date = act.get("startTimeLocal", "")[:10]
            name = act.get("activityName", "Unknown")
            act_type = act.get("activityType", {}).get("typeKey", "")
            duration = act.get("duration", 0) / 60
            distance = act.get("distance", 0) / 1000
            calories = act.get("calories", 0)
            avg_hr = act.get("averageHR", "N/A")
            max_hr = act.get("maxHR", "N/A")

            lines.append(
                f"- **{date} {name}** ({act_type}): {duration:.0f} min, {distance:.2f} km, "
                f"{calories} kcal, HR {avg_hr}/{max_hr}"
            )
        lines.append("")

    lines.extend([
        "## Data Availability",
        f"- Daily Stats: {len(data.get('daily_stats', []))} days",
        f"- Sleep Data: {len(data.get('sleep_history', []))} nights",
        f"- Heart Rate: {len(data.get('heart_rate_history', []))} days",
        f"- HRV: {len(data.get('hrv_history', []))} days",
        f"- Stress: {len(data.get('stress_history', []))} days",
        f"- SpO2: {len(data.get('spo2_history', []))} days",
        f"- Respiration: {len(data.get('respiration_history', []))} days",
        f"- Activities: {len(data.get('activities', []))} activities",
        f"- Weight: {len(data.get('weight_history', []))} entries",
        ""
    ])

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"Summary saved to {summary_file}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Fetch Garmin health data (optimized)")
    parser.add_argument("--days", type=int, default=30, help="Days of history to fetch")
    args = parser.parse_args()

    total_start = time.time()
    fetch_all_data(days_history=args.days)
    print(f"\n=== TOTAL TIME: {time.time() - total_start:.1f} seconds ===")
