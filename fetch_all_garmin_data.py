#!/usr/bin/env python3
"""
Fetch ALL health data from Garmin Connect including full history.
Saves to JSON for TypingMind Knowledge Base integration.
"""

import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from garminconnect import Garmin

# Load credentials
load_dotenv()
EMAIL = os.getenv("GARMIN_EMAIL")
PASSWORD = os.getenv("GARMIN_PASSWORD")

DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

SESSION_FILE = Path(__file__).parent / ".garmin_session"


def get_client():
    """Initialize Garmin client with session caching"""
    client = Garmin(EMAIL, PASSWORD)

    # Try to load existing session
    if SESSION_FILE.exists():
        try:
            client.login(str(SESSION_FILE))
            print("Logged in using saved session")
            return client
        except Exception as e:
            print(f"Session expired, re-authenticating: {e}")

    # Fresh login
    client.login()
    client.garth.dump(str(SESSION_FILE))
    print("Fresh login successful")
    return client


def safe_call(func, *args, default=None, **kwargs):
    """Safely call API method and handle errors"""
    try:
        result = func(*args, **kwargs)
        return result
    except Exception as e:
        print(f"  Warning: {func.__name__} failed: {e}")
        return default


def fetch_all_data(days_history=30):
    """Fetch ALL available health data with history"""
    client = get_client()
    today = datetime.now().date()
    start_date = today - timedelta(days=days_history)

    print(f"\n=== Fetching Garmin data from {start_date} to {today} ===\n")

    data = {
        "fetched_at": datetime.now().isoformat(),
        "date_range": {
            "start": str(start_date),
            "end": str(today),
            "days": days_history
        }
    }

    # =============================================
    # USER PROFILE
    # =============================================
    print("Fetching user profile...")
    data["user_profile"] = safe_call(client.get_user_profile, default={})
    data["user_settings"] = safe_call(client.get_userprofile_settings, default={})
    data["unit_system"] = safe_call(client.get_unit_system, default={})

    # =============================================
    # DEVICES
    # =============================================
    print("Fetching devices...")
    data["devices"] = safe_call(client.get_devices, default=[])
    data["device_last_used"] = safe_call(client.get_device_last_used, default={})

    # =============================================
    # DAILY STATS HISTORY
    # =============================================
    print(f"Fetching daily stats for {days_history} days...")
    data["daily_stats"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        stats = safe_call(client.get_stats, date.isoformat(), default={})
        if stats:
            stats["date"] = str(date)
            data["daily_stats"].append(stats)
    print(f"  Got {len(data['daily_stats'])} days of stats")

    # =============================================
    # SLEEP HISTORY
    # =============================================
    print(f"Fetching sleep data for {days_history} days...")
    data["sleep_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        sleep = safe_call(client.get_sleep_data, date.isoformat(), default=None)
        if sleep and sleep.get("dailySleepDTO"):
            sleep_data = sleep["dailySleepDTO"]
            sleep_data["date"] = str(date)
            sleep_data["sleep_levels"] = sleep.get("sleepLevels", [])
            data["sleep_history"].append(sleep_data)
    print(f"  Got {len(data['sleep_history'])} nights of sleep data")

    # =============================================
    # HEART RATE HISTORY
    # =============================================
    print(f"Fetching heart rate data for {days_history} days...")
    data["heart_rate_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        hr = safe_call(client.get_heart_rates, date.isoformat(), default=None)
        if hr:
            hr["date"] = str(date)
            data["heart_rate_history"].append(hr)
    print(f"  Got {len(data['heart_rate_history'])} days of HR data")

    # =============================================
    # HRV (Heart Rate Variability)
    # =============================================
    print(f"Fetching HRV data for {days_history} days...")
    data["hrv_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        hrv = safe_call(client.get_hrv_data, date.isoformat(), default=None)
        if hrv:
            hrv["date"] = str(date)
            data["hrv_history"].append(hrv)
    print(f"  Got {len(data['hrv_history'])} days of HRV data")

    # =============================================
    # STRESS DATA
    # =============================================
    print(f"Fetching stress data for {days_history} days...")
    data["stress_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        stress = safe_call(client.get_stress_data, date.isoformat(), default=None)
        if stress:
            stress["date"] = str(date)
            data["stress_history"].append(stress)
    print(f"  Got {len(data['stress_history'])} days of stress data")

    # =============================================
    # BODY BATTERY
    # =============================================
    print(f"Fetching body battery for {days_history} days...")
    data["body_battery_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        bb = safe_call(client.get_body_battery, date.isoformat(), default=None)
        if bb:
            bb_data = {"date": str(date), "data": bb}
            data["body_battery_history"].append(bb_data)
    print(f"  Got {len(data['body_battery_history'])} days of body battery")

    # =============================================
    # RESPIRATION
    # =============================================
    print(f"Fetching respiration data for {days_history} days...")
    data["respiration_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        resp = safe_call(client.get_respiration_data, date.isoformat(), default=None)
        if resp:
            resp["date"] = str(date)
            data["respiration_history"].append(resp)
    print(f"  Got {len(data['respiration_history'])} days of respiration data")

    # =============================================
    # SPO2 (Blood Oxygen)
    # =============================================
    print(f"Fetching SpO2 data for {days_history} days...")
    data["spo2_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        spo2 = safe_call(client.get_spo2_data, date.isoformat(), default=None)
        if spo2:
            spo2["date"] = str(date)
            data["spo2_history"].append(spo2)
    print(f"  Got {len(data['spo2_history'])} days of SpO2 data")

    # =============================================
    # STEPS DATA (detailed)
    # =============================================
    print(f"Fetching detailed steps for {days_history} days...")
    data["steps_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        steps = safe_call(client.get_steps_data, date.isoformat(), default=None)
        if steps:
            steps_data = {"date": str(date), "data": steps}
            data["steps_history"].append(steps_data)
    print(f"  Got {len(data['steps_history'])} days of steps data")

    # =============================================
    # FLOORS CLIMBED
    # =============================================
    print(f"Fetching floors data for {days_history} days...")
    data["floors_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        floors = safe_call(client.get_floors, date.isoformat(), default=None)
        if floors:
            floors_data = {"date": str(date), "data": floors}
            data["floors_history"].append(floors_data)
    print(f"  Got {len(data['floors_history'])} days of floors data")

    # =============================================
    # INTENSITY MINUTES
    # =============================================
    print(f"Fetching intensity minutes for {days_history} days...")
    data["intensity_minutes_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        intensity = safe_call(client.get_intensity_minutes_data, date.isoformat(), default=None)
        if intensity:
            intensity["date"] = str(date)
            data["intensity_minutes_history"].append(intensity)
    print(f"  Got {len(data['intensity_minutes_history'])} days of intensity data")

    # =============================================
    # HYDRATION
    # =============================================
    print(f"Fetching hydration data for {days_history} days...")
    data["hydration_history"] = []
    for i in range(days_history + 1):
        date = today - timedelta(days=i)
        hydration = safe_call(client.get_hydration_data, date.isoformat(), default=None)
        if hydration:
            hydration["date"] = str(date)
            data["hydration_history"].append(hydration)
    print(f"  Got {len(data['hydration_history'])} days of hydration data")

    # =============================================
    # BODY COMPOSITION / WEIGHT
    # =============================================
    print("Fetching body composition history...")
    data["weight_history"] = safe_call(
        client.get_weigh_ins,
        start_date.isoformat(),
        today.isoformat(),
        default=[]
    )
    print(f"  Got {len(data.get('weight_history', []))} weight entries")

    data["body_composition"] = safe_call(
        client.get_body_composition,
        today.isoformat(),
        default={}
    )

    # =============================================
    # BLOOD PRESSURE
    # =============================================
    print("Fetching blood pressure data...")
    data["blood_pressure"] = safe_call(
        client.get_blood_pressure,
        start_date.isoformat(),
        today.isoformat(),
        default=[]
    )
    print(f"  Got {len(data.get('blood_pressure', []))} blood pressure entries")

    # =============================================
    # ACTIVITIES (Training History) - ALL of them
    # =============================================
    print("Fetching ALL activities (this may take a while)...")
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
    print(f"  Got {len(data.get('activities', []))} TOTAL activities")

    # Get activity types for reference
    data["activity_types"] = safe_call(client.get_activity_types, default=[])

    # =============================================
    # TRAINING STATUS & METRICS
    # =============================================
    print("Fetching training status...")
    data["training_status"] = safe_call(client.get_training_status, today.isoformat(), default={})
    data["training_readiness"] = safe_call(client.get_training_readiness, today.isoformat(), default={})
    data["max_metrics"] = safe_call(client.get_max_metrics, today.isoformat(), default={})
    data["fitness_age"] = safe_call(client.get_fitnessage_data, today.isoformat(), default={})
    data["endurance_score"] = safe_call(client.get_endurance_score, today.isoformat(), default={})
    data["hill_score"] = safe_call(client.get_hill_score, today.isoformat(), default={})
    data["race_predictions"] = safe_call(client.get_race_predictions, default={})
    data["lactate_threshold"] = safe_call(client.get_lactate_threshold, default={})

    # =============================================
    # PERSONAL RECORDS
    # =============================================
    print("Fetching personal records...")
    data["personal_records"] = safe_call(client.get_personal_record, default={})

    # =============================================
    # GOALS
    # =============================================
    print("Fetching goals...")
    data["goals"] = safe_call(client.get_goals, "all", default={})

    # =============================================
    # BADGES & CHALLENGES
    # =============================================
    print("Fetching badges and challenges...")
    data["earned_badges"] = safe_call(client.get_earned_badges, default=[])
    data["badge_challenges"] = safe_call(client.get_badge_challenges, default=[])

    # =============================================
    # GEAR
    # =============================================
    print("Fetching gear...")
    data["gear"] = safe_call(client.get_gear, default=[])

    # =============================================
    # WORKOUTS
    # =============================================
    print("Fetching workouts...")
    data["workouts"] = safe_call(client.get_workouts, default=[])

    # Save FULL data to JSON (local only, excluded from git)
    json_file = DATA_DIR / "garmin_full_data.json"
    with open(json_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)

    print(f"\n=== Full data saved to {json_file} ===")
    print(f"File size: {json_file.stat().st_size / 1024 / 1024:.2f} MB")

    # Save COMPACT data for GitHub (essential data only)
    compact_data = generate_compact_data(data)
    compact_file = DATA_DIR / "garmin_health.json"
    with open(compact_file, "w", encoding="utf-8") as f:
        json.dump(compact_data, f, indent=2, ensure_ascii=False, default=str)

    print(f"Compact data saved to {compact_file}")
    print(f"Compact file size: {compact_file.stat().st_size / 1024:.2f} KB")

    # Generate summary
    generate_summary(data)

    return data


def generate_compact_data(data):
    """Generate compact JSON for GitHub with essential data only"""
    compact = {
        "fetched_at": data.get("fetched_at"),
        "date_range": data.get("date_range"),
        "total_activities": len(data.get("activities", [])),
        "oldest_activity": None,
        "newest_activity": None,
    }

    # Add oldest/newest activity dates
    activities = data.get("activities", [])
    if activities:
        compact["newest_activity"] = activities[0].get("startTimeLocal", "")[:10]
        compact["oldest_activity"] = activities[-1].get("startTimeLocal", "")[:10]

    # Today's stats (compact)
    if data.get("daily_stats") and len(data["daily_stats"]) > 0:
        today = data["daily_stats"][0]
        compact["today"] = {
            "date": today.get("date"),
            "steps": today.get("totalSteps"),
            "distance_km": round(today.get("totalDistanceMeters", 0) / 1000, 2),
            "calories": today.get("totalKilocalories"),
            "active_calories": today.get("activeKilocalories"),
            "resting_hr": today.get("restingHeartRate"),
            "min_hr": today.get("minHeartRate"),
            "max_hr": today.get("maxHeartRate"),
            "stress_avg": today.get("averageStressLevel"),
            "body_battery_high": today.get("bodyBatteryChargedValue"),
            "body_battery_low": today.get("bodyBatteryDrainedValue"),
            "floors_climbed": today.get("floorsAscended"),
            "intensity_minutes": today.get("intensityMinutesGoal"),
        }

    # Last night's sleep (compact)
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

    # HRV (compact)
    if data.get("hrv_history") and len(data["hrv_history"]) > 0:
        hrv = data["hrv_history"][0]
        summary = hrv.get("hrvSummary", {})
        compact["hrv"] = {
            "weekly_avg": summary.get("weeklyAvg"),
            "last_night": summary.get("lastNight"),
            "status": summary.get("status"),
        }

    # Recent activities (last 20, compact)
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

    # Activity summary by type (all time)
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

    # Round values
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

    # Personal records
    if data.get("personal_records"):
        compact["personal_records"] = data["personal_records"]

    # Data availability
    compact["data_availability"] = {
        "daily_stats_days": len(data.get("daily_stats", [])),
        "sleep_nights": len(data.get("sleep_history", [])),
        "heart_rate_days": len(data.get("heart_rate_history", [])),
        "hrv_days": len(data.get("hrv_history", [])),
        "stress_days": len(data.get("stress_history", [])),
        "body_battery_days": len(data.get("body_battery_history", [])),
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

    # User info
    if data.get("user_profile"):
        profile = data["user_profile"]
        lines.extend([
            "## User Profile",
            f"- **Name:** {profile.get('displayName', 'N/A')}",
            f"- **Email:** {profile.get('userName', 'N/A')}",
            ""
        ])

    # Today's stats
    if data.get("daily_stats") and len(data["daily_stats"]) > 0:
        today_stats = data["daily_stats"][0]
        lines.extend([
            "## Today's Stats",
            f"- **Steps:** {today_stats.get('totalSteps', 'N/A'):,}",
            f"- **Distance:** {today_stats.get('totalDistanceMeters', 0) / 1000:.2f} km",
            f"- **Calories:** {today_stats.get('totalKilocalories', 'N/A')}",
            f"- **Active Calories:** {today_stats.get('activeKilocalories', 'N/A')}",
            f"- **Resting HR:** {today_stats.get('restingHeartRate', 'N/A')} bpm",
            f"- **Min/Max HR:** {today_stats.get('minHeartRate', 'N/A')}/{today_stats.get('maxHeartRate', 'N/A')} bpm",
            f"- **Stress Level:** {today_stats.get('averageStressLevel', 'N/A')}",
            f"- **Body Battery:** {today_stats.get('bodyBatteryDrainedValue', 'N/A')} - {today_stats.get('bodyBatteryChargedValue', 'N/A')}",
            f"- **Floors Climbed:** {today_stats.get('floorsAscended', 'N/A')}",
            ""
        ])

    # Last night's sleep
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

    # HRV
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

    # Training status
    if data.get("training_status"):
        ts = data["training_status"]
        lines.extend([
            "## Training Status",
            f"- **VO2 Max:** {ts.get('vo2MaxPreciseValue', 'N/A')}",
            f"- **Training Load:** {ts.get('acuteTrainingLoad', 'N/A')}",
            f"- **Recovery Time:** {ts.get('recoveryTimeInMinutes', 0) / 60:.0f} hours",
            ""
        ])

    # Recent activities
    if data.get("activities"):
        lines.extend([
            "## Recent Activities",
            ""
        ])
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

    # Data availability summary
    lines.extend([
        "## Data Availability",
        f"- Daily Stats: {len(data.get('daily_stats', []))} days",
        f"- Sleep Data: {len(data.get('sleep_history', []))} nights",
        f"- Heart Rate: {len(data.get('heart_rate_history', []))} days",
        f"- HRV: {len(data.get('hrv_history', []))} days",
        f"- Stress: {len(data.get('stress_history', []))} days",
        f"- Body Battery: {len(data.get('body_battery_history', []))} days",
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
    parser = argparse.ArgumentParser(description="Fetch Garmin health data")
    parser.add_argument("--days", type=int, default=30, help="Days of history to fetch")
    args = parser.parse_args()

    fetch_all_data(days_history=args.days)
